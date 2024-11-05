# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from contextlib import asynccontextmanager
import os
import glob
import argparse
import time
import traceback
from typing import Annotated, List, Optional, Union
from urllib.parse import urljoin
from fastapi import FastAPI, Header, Request, Form
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    StreamingResponse,
    Response,
    JSONResponse,
)
from fastapi.templating import Jinja2Templates
from fastapi_utils.tasks import repeat_every

import git
import httpx

from olah.proxy.commits import commits_generator
from olah.proxy.pathsinfo import pathsinfo_generator
from olah.proxy.tree import tree_generator
from olah.utils.disk_utils import convert_bytes_to_human_readable, convert_to_bytes, get_folder_size, sort_files_by_access_time, sort_files_by_modify_time, sort_files_by_size
from olah.utils.url_utils import clean_path
from olah.utils.zip_utils import decompress_data

BASE_SETTINGS = False
if not BASE_SETTINGS:
    try:
        from pydantic import BaseSettings
        BASE_SETTINGS = True
    except ImportError:
        BASE_SETTINGS = False

if not BASE_SETTINGS:
    try:
        from pydantic_settings import BaseSettings
        BASE_SETTINGS = True
    except ImportError:
        BASE_SETTINGS = False

if not BASE_SETTINGS:
    raise Exception("Cannot import BaseSettings from pydantic or pydantic-settings")

from olah.configs import OlahConfig
from olah.errors import error_repo_not_found, error_page_not_found, error_revision_not_found
from olah.mirror.repos import LocalMirrorRepo
from olah.proxy.files import cdn_file_get_generator, file_get_generator
from olah.proxy.lfs import lfs_get_generator, lfs_head_generator
from olah.proxy.meta import meta_generator
from olah.utils.rule_utils import check_proxy_rules_hf, get_org_repo
from olah.utils.repo_utils import (
    check_commit_hf,
    get_commit_hf,
    get_newest_commit_hf,
    parse_org_repo,
)
from olah.constants import REPO_TYPES_MAPPING
from olah.utils.logging import build_logger

logger = None

# ======================
# Utilities
# ======================
async def check_connection(url: str) -> bool:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method="HEAD",
                url=url,
                timeout=10,
            )
        if response.status_code != 200:
            return False
        else:
            return True
    except httpx.TimeoutException:
        return False


@repeat_every(seconds=60 * 5)
async def check_hf_connection() -> None:
    if app.app_settings.config.offline:
        return
    scheme = app.app_settings.config.hf_scheme
    netloc = app.app_settings.config.hf_netloc
    hf_online_status = await check_connection(
        f"{scheme}://{netloc}/datasets/Salesforce/wikitext/resolve/main/.gitattributes"
    )
    if not hf_online_status:
        logger.error("Failed to reach Huggingface Site.")


@repeat_every(seconds=60 * 60)
async def check_disk_usage() -> None:
    if app.app_settings.config.offline:
        return
    if app.app_settings.config.cache_size_limit is None:
        return

    limit_size = app.app_settings.config.cache_size_limit
    current_size = get_folder_size(app.app_settings.config.repos_path)

    limit_size_h = convert_bytes_to_human_readable(limit_size)
    current_size_h = convert_bytes_to_human_readable(current_size)

    if current_size < limit_size:
        return
    logger.warning(
        f"Cache size exceeded! Limit: {limit_size_h}, Current: {current_size_h}."
    )
    logger.info("Cleaning...")
    files_path = os.path.join(app.app_settings.config.repos_path, "files")
    lfs_path = os.path.join(app.app_settings.config.repos_path, "lfs")

    if app.app_settings.config.cache_clean_strategy == "LRU":
        files = sort_files_by_access_time(files_path) + sort_files_by_access_time(
            lfs_path
        )
        files = sorted(files, key=lambda x: x[1])
    elif app.app_settings.config.cache_clean_strategy == "FIFO":
        files = sort_files_by_modify_time(files_path) + sort_files_by_modify_time(
            lfs_path
        )
        files = sorted(files, key=lambda x: x[1])
    elif app.app_settings.config.cache_clean_strategy == "LARGE_FIRST":
        files = sort_files_by_size(files_path) + sort_files_by_size(lfs_path)
        files = sorted(files, key=lambda x: x[1], reverse=True)

    for filepath, index in files:
        if current_size < limit_size:
            break
        filesize = os.path.getsize(filepath)
        os.remove(filepath)
        current_size -= filesize
        logger.info(f"Remove file: {filepath}. File Size: {convert_bytes_to_human_readable(filesize)}")

    current_size = get_folder_size(app.app_settings.config.repos_path)
    current_size_h = convert_bytes_to_human_readable(current_size)
    logger.info(f"Cleaning finished. Limit: {limit_size_h}, Current: {current_size_h}.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # TODO: Check repo cache path
    await check_hf_connection()
    await check_disk_usage()
    yield


# ======================
# Application
# ======================
code_file_path = os.path.abspath(__file__)
app = FastAPI(lifespan=lifespan, debug=False)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(code_file_path), "..", "static"))


class AppSettings(BaseSettings):
    # The address of the model controller.
    config: OlahConfig = OlahConfig()


# ======================
# Exception handlers
# ======================
@app.exception_handler(404)
async def custom_404_handler(_, __):
    return error_page_not_found()


# ======================
# File Meta Info API Hooks
# See also: https://huggingface.co/docs/hub/api#repo-listing-api
# ======================
async def meta_proxy_common(repo_type: str, org: str, repo: str, commit: str, method: str, authorization: Optional[str]) -> Response:
    # FIXME: do not show the private repos to other user besides owner, even though the repo was cached
    if repo_type not in REPO_TYPES_MAPPING.keys():
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    # Check Mirror Path
    for mirror_path in app.app_settings.config.mirrors_path:
        try:
            git_path = os.path.join(mirror_path, repo_type, org, repo)
            if os.path.exists(git_path):
                local_repo = LocalMirrorRepo(git_path, repo_type, org, repo)
                meta_data = local_repo.get_meta(commit)
                if meta_data is None:
                    continue
                return JSONResponse(content=meta_data)
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue

    # Proxy the HF File Meta
    try:
        if not app.app_settings.config.offline:
            if not await check_commit_hf(app, repo_type, org, repo, commit=None,
                authorization=authorization,
            ):
                return error_repo_not_found()
            if not await check_commit_hf(app, repo_type, org, repo, commit=commit,
                authorization=authorization,
            ):
                return error_revision_not_found(revision=commit)
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit=commit,
            authorization=authorization,
        )
        if commit_sha is None:
            return error_repo_not_found()
        # if branch name and online mode, refresh branch info
        if not app.app_settings.config.offline and commit_sha != commit:
            generator = meta_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
            async for _ in generator:
                pass
            generator = meta_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
        else:
            generator = meta_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                override_cache=False,
                method=method,
                authorization=authorization,
            )
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.head("/api/{repo_type}/{org_repo}")
@app.get("/api/{repo_type}/{org_repo}")
async def meta_proxy(repo_type: str, org_repo: str, request: Request):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()
    if not app.app_settings.config.offline:
        new_commit = await get_newest_commit_hf(
            app,
            repo_type,
            org,
            repo,
            authorization=request.headers.get("authorization", None),
        )
        if new_commit is None:
            return error_repo_not_found()
    else:
        new_commit = "main"
    return await meta_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=new_commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@app.head("/api/{repo_type}/{org}/{repo}")
@app.get("/api/{repo_type}/{org}/{repo}")
async def meta_proxy(repo_type: str, org: str, repo: str, request: Request):
    if not app.app_settings.config.offline:
        new_commit = await get_newest_commit_hf(
            app,
            repo_type,
            org,
            repo,
            authorization=request.headers.get("authorization", None),
        )
        if new_commit is None:
            return error_repo_not_found()
    else:
        new_commit = "main"
    return await meta_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=new_commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@app.head("/api/{repo_type}/{org}/{repo}/revision/{commit}")
@app.get("/api/{repo_type}/{org}/{repo}/revision/{commit}")
async def meta_proxy_commit2(
    repo_type: str, org: str, repo: str, commit: str, request: Request
):
    return await meta_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@app.head("/api/{repo_type}/{org_repo}/revision/{commit}")
@app.get("/api/{repo_type}/{org_repo}/revision/{commit}")
async def meta_proxy_commit(
    repo_type: str, org_repo: str, commit: str, request: Request
):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    return await meta_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


# Git Tree
async def tree_proxy_common(
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    path: str,
    recursive: bool,
    expand: bool,
    method: str,
    authorization: Optional[str]
) -> Response:
    # FIXME: do not show the private repos to other user besides owner, even though the repo was cached
    path = clean_path(path)
    if repo_type not in REPO_TYPES_MAPPING.keys():
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    # Check Mirror Path
    for mirror_path in app.app_settings.config.mirrors_path:
        try:
            git_path = os.path.join(mirror_path, repo_type, org, repo)
            if os.path.exists(git_path):
                local_repo = LocalMirrorRepo(git_path, repo_type, org, repo)
                tree_data = local_repo.get_tree(commit, path, recursive=recursive, expand=expand)
                if tree_data is None:
                    continue
                return JSONResponse(content=tree_data)
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue

    # Proxy the HF File Meta
    try:
        if not app.app_settings.config.offline:
            if not await check_commit_hf(app, repo_type, org, repo, commit=None,
                authorization=authorization,
            ):
                return error_repo_not_found()
            if not await check_commit_hf(app, repo_type, org, repo, commit=commit,
                authorization=authorization,
            ):
                return error_revision_not_found(revision=commit)
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit=commit,
            authorization=authorization,
        )
        if commit_sha is None:
            return error_repo_not_found()
        # if branch name and online mode, refresh branch info
        if not app.app_settings.config.offline and commit_sha != commit:
            generator = tree_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit,
                path=path,
                recursive=recursive,
                expand=expand,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
            async for _ in generator:
                pass
            generator = tree_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                path=path,
                recursive=recursive,
                expand=expand,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
        else:
            generator = tree_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                path=path,
                recursive=recursive,
                expand=expand,
                override_cache=False,
                method=method,
                authorization=authorization,
            )

        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, status_code=status_code, headers=headers)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.head("/api/{repo_type}/{org}/{repo}/tree/{commit}/{file_path:path}")
@app.get("/api/{repo_type}/{org}/{repo}/tree/{commit}/{file_path:path}")
async def tree_proxy_commit2(
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    request: Request,
    recursive: bool = False,
    expand: bool=False,
):
    return await tree_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        path=file_path,
        recursive=recursive,
        expand=expand,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@app.head("/api/{repo_type}/{org_repo}/tree/{commit}/{file_path:path}")
@app.get("/api/{repo_type}/{org_repo}/tree/{commit}/{file_path:path}")
async def tree_proxy_commit(
    repo_type: str,
    org_repo: str,
    commit: str,
    file_path: str,
    request: Request,
    recursive: bool = False,
    expand: bool=False,
):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    return await tree_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        path=file_path,
        recursive=recursive,
        expand=expand,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )

# Git Pathsinfo
async def pathsinfo_proxy_common(repo_type: str, org: str, repo: str, commit: str, paths: List[str], method: str, authorization: Optional[str]) -> Response:
    # TODO: the head method of meta apis
    # FIXME: do not show the private repos to other user besides owner, even though the repo was cached
    paths = [clean_path(path) for path in paths]
    if repo_type not in REPO_TYPES_MAPPING.keys():
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    # Check Mirror Path
    for mirror_path in app.app_settings.config.mirrors_path:
        try:
            git_path = os.path.join(mirror_path, repo_type, org, repo)
            if os.path.exists(git_path):
                local_repo = LocalMirrorRepo(git_path, repo_type, org, repo)
                pathsinfo_data = local_repo.get_pathinfos(commit, paths)
                if pathsinfo_data is None:
                    continue
                return JSONResponse(content=pathsinfo_data)
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue

    # Proxy the HF File pathsinfo
    try:
        if not app.app_settings.config.offline:
            if not await check_commit_hf(app, repo_type, org, repo, commit=None,
                authorization=authorization,
            ):
                return error_repo_not_found()
            if not await check_commit_hf(app, repo_type, org, repo, commit=commit,
                authorization=authorization,
            ):
                return error_revision_not_found(revision=commit)
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit=commit,
            authorization=authorization,
        )
        if commit_sha is None:
            return error_repo_not_found()
        if not app.app_settings.config.offline and commit_sha != commit:
            generator = pathsinfo_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit,
                paths=paths,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
            async for _ in generator:
                pass
            generator = pathsinfo_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                paths=paths,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
        else:
            generator = pathsinfo_generator(
                app,
                repo_type,
                org,
                repo,
                commit_sha,
                paths,
                override_cache=False,
                method=method,
                authorization=authorization,
            )
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, status_code=status_code, headers=headers)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.head("/api/{repo_type}/{org}/{repo}/paths-info/{commit}")
@app.post("/api/{repo_type}/{org}/{repo}/paths-info/{commit}")
async def pathsinfo_proxy_commit2(
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    paths: Annotated[List[str], Form()],
    request: Request,
):
    return await pathsinfo_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        paths=paths,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@app.head("/api/{repo_type}/{org_repo}/paths-info/{commit}")
@app.post("/api/{repo_type}/{org_repo}/paths-info/{commit}")
async def pathsinfo_proxy_commit(
    repo_type: str,
    org_repo: str,
    commit: str,
    paths: Annotated[List[str], Form()],
    request: Request,
):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    return await pathsinfo_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        paths=paths,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


# Git Commits
async def commits_proxy_common(repo_type: str, org: str, repo: str, commit: str, method: str, authorization: Optional[str]) -> Response:
    # FIXME: do not show the private repos to other user besides owner, even though the repo was cached
    if repo_type not in REPO_TYPES_MAPPING.keys():
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    # Check Mirror Path
    for mirror_path in app.app_settings.config.mirrors_path:
        try:
            git_path = os.path.join(mirror_path, repo_type, org, repo)
            if os.path.exists(git_path):
                local_repo = LocalMirrorRepo(git_path, repo_type, org, repo)
                commits_data = local_repo.get_commits(commit)
                if commits_data is None:
                    continue
                return JSONResponse(content=commits_data)
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue

    # Proxy the HF File Commits
    try:
        if not app.app_settings.config.offline:
            if not await check_commit_hf(app, repo_type, org, repo, commit=None,
                authorization=authorization,
            ):
                return error_repo_not_found()
            if not await check_commit_hf(app, repo_type, org, repo, commit=commit,
                authorization=authorization,
            ):
                return error_revision_not_found(revision=commit)
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit=commit,
            authorization=authorization,
        )
        if commit_sha is None:
            return error_repo_not_found()
        # if branch name and online mode, refresh branch info
        if not app.app_settings.config.offline and commit_sha != commit:
            generator = commits_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
            async for _ in generator:
                pass
            generator = commits_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                override_cache=True,
                method=method,
                authorization=authorization,
            )
        else:
            generator = commits_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=commit_sha,
                override_cache=False,
                method=method,
                authorization=authorization,
            )
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, status_code=status_code, headers=headers)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.head("/api/{repo_type}/{org}/{repo}/commits/{commit}")
@app.get("/api/{repo_type}/{org}/{repo}/commits/{commit}")
async def commits_proxy_commit2(
    repo_type: str, org: str, repo: str, commit: str, request: Request
):
    return await commits_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@app.head("/api/{repo_type}/{org_repo}/commits/{commit}")
@app.get("/api/{repo_type}/{org_repo}/commits/{commit}")
async def commits_proxy_commit(
    repo_type: str, org_repo: str, commit: str, request: Request
):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    return await commits_proxy_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


# ======================
# Authentication API Hooks
# ======================
@app.get("/api/whoami-v2")
async def whoami_v2(request: Request):
    """
    Sensitive Information!!! 
    """
    new_headers = {k.lower(): v for k, v in request.headers.items()}
    new_headers["host"] = app.app_settings.config.hf_netloc
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method="GET",
            url=urljoin(app.app_settings.config.hf_url_base(), "/api/whoami-v2"),
            headers=new_headers,
            timeout=10,
        )
    # final_content = decompress_data(response.headers.get("content-encoding", None))
    response_headers = {k.lower(): v for k, v in response.headers.items()}
    if "content-encoding" in response_headers:
        response_headers.pop("content-encoding")
    if "content-length" in response_headers:
        response_headers.pop("content-length")
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=response_headers,
    )


# ======================
# File Head Hooks
# ======================
async def file_head_common(
    repo_type: str, org: str, repo: str, commit: str, file_path: str, request: Request
) -> Response:
    if repo_type not in REPO_TYPES_MAPPING.keys():
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()

    # Check Mirror Path
    for mirror_path in app.app_settings.config.mirrors_path:
        try:
            git_path = os.path.join(mirror_path, repo_type, org, repo)
            if os.path.exists(git_path):
                local_repo = LocalMirrorRepo(git_path, repo_type, org, repo)
                head = local_repo.get_file_head(commit_hash=commit, path=file_path)
                if head is None:
                    continue
                return Response(headers=head)
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue

    # Proxy the HF File Head
    try:
        if not app.app_settings.config.offline and not await check_commit_hf(
            app,
            repo_type,
            org,
            repo,
            commit=commit,
            authorization=request.headers.get("authorization", None),
        ):
            return error_repo_not_found()
        commit_sha = await get_commit_hf(
            app,
            repo_type,
            org,
            repo,
            commit=commit,
            authorization=request.headers.get("authorization", None),
        )
        if commit_sha is None:
            return error_repo_not_found()
        generator = await file_get_generator(
            app,
            repo_type,
            org,
            repo,
            commit_sha,
            file_path=file_path,
            method="HEAD",
            request=request,
        )
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers, status_code=status_code)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.head("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_head3(
    repo_type: str, org: str, repo: str, commit: str, file_path: str, request: Request
):
    return await file_head_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@app.head("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_head2(
    org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request
):
    if org_or_repo_type in REPO_TYPES_MAPPING.keys():
        repo_type: str = org_or_repo_type
        org, repo = parse_org_repo(repo_name)
        if org is None and repo is None:
            return error_repo_not_found()
    else:
        repo_type: str = "models"
        org, repo = org_or_repo_type, repo_name

    return await file_head_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@app.head("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_head(org_repo: str, commit: str, file_path: str, request: Request):
    repo_type: str = "models"
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()
    return await file_head_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@app.head("/{org_repo}/{hash_file}")
@app.head("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_head(org_repo: str, hash_file: str, request: Request, repo_type: str = "models"):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()

    try:
        generator = await cdn_file_get_generator(app, repo_type, org, repo, hash_file, method="HEAD", request=request)
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers, status_code=status_code)
    except httpx.ConnectTimeout:
        return Response(status_code=504)


# ======================
# File Hooks
# ======================
async def file_get_common(
    repo_type: str, org: str, repo: str, commit: str, file_path: str, request: Request
) -> Response:
    if repo_type not in REPO_TYPES_MAPPING.keys():
        return error_page_not_found()
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    # Check Mirror Path
    for mirror_path in app.app_settings.config.mirrors_path:
        try:
            git_path = os.path.join(mirror_path, repo_type, org, repo)
            if os.path.exists(git_path):
                local_repo = LocalMirrorRepo(git_path, repo_type, org, repo)
                content_stream = local_repo.get_file(commit_hash=commit, path=file_path)
                if content_stream is None:
                    continue
                return StreamingResponse(content_stream)
        except git.exc.InvalidGitRepositoryError:
            logger.warning(f"Local repository {git_path} is not a valid git reposity.")
            continue
    try:
        if not app.app_settings.config.offline and not await check_commit_hf(
            app,
            repo_type,
            org,
            repo,
            commit=commit,
            authorization=request.headers.get("authorization", None),
        ):
            return error_repo_not_found()
        commit_sha = await get_commit_hf(
            app,
            repo_type,
            org,
            repo,
            commit=commit,
            authorization=request.headers.get("authorization", None),
        )
        if commit_sha is None:
            return error_repo_not_found()
        generator = await file_get_generator(
            app,
            repo_type,
            org,
            repo,
            commit_sha,
            file_path=file_path,
            method="GET",
            request=request,
        )
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers, status_code=status_code)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.get("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_get3(
    org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str
):
    return await file_get_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@app.get("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_get2(
    org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request
):
    if org_or_repo_type in REPO_TYPES_MAPPING.keys():
        repo_type: str = org_or_repo_type
        org, repo = parse_org_repo(repo_name)
        if org is None and repo is None:
            return error_repo_not_found()
    else:
        repo_type: str = "models"
        org, repo = org_or_repo_type, repo_name

    return await file_get_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@app.get("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_get(org_repo: str, commit: str, file_path: str, request: Request):
    repo_type: str = "models"
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    return await file_get_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@app.get("/{org_repo}/{hash_file}")
@app.get("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_get(
    org_repo: str, hash_file: str, request: Request, repo_type: str = "models"
):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    try:
        generator = await cdn_file_get_generator(
            app, repo_type, org, repo, hash_file, method="GET", request=request
        )
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers, status_code=status_code)
    except httpx.ConnectTimeout:
        return Response(status_code=504)


# ======================
# LFS Hooks
# ======================
@app.head("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_head(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    try:
        generator = await lfs_head_generator(app, dir1, dir2, hash_repo, hash_file, request)
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers, status_code=status_code)
    except httpx.ConnectTimeout:
        return Response(status_code=504)

@app.get("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_get(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    try:
        generator = await lfs_get_generator(app, dir1, dir2, hash_repo, hash_file, request)
        status_code = await generator.__anext__()
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers, status_code=status_code)
    except httpx.ConnectTimeout:
        return Response(status_code=504)


# ======================
# Web Page Hooks
# ======================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "scheme": app.app_settings.config.mirror_scheme,
            "netloc": app.app_settings.config.mirror_netloc,
        },
    )

@app.get("/repos", response_class=HTMLResponse)
async def repos(request: Request):
    datasets_repos = glob.glob(os.path.join(app.app_settings.config.repos_path, "api/datasets/*/*"))
    models_repos = glob.glob(os.path.join(app.app_settings.config.repos_path, "api/models/*/*"))
    spaces_repos = glob.glob(os.path.join(app.app_settings.config.repos_path, "api/spaces/*/*"))
    datasets_repos = [get_org_repo(*repo.split("/")[-2:]) for repo in datasets_repos]
    models_repos = [get_org_repo(*repo.split("/")[-2:]) for repo in models_repos]
    spaces_repos = [get_org_repo(*repo.split("/")[-2:]) for repo in spaces_repos]

    return templates.TemplateResponse(
        "repos.html",
        {
            "request": request,
            "datasets_repos": datasets_repos,
            "models_repos": models_repos,
            "spaces_repos": spaces_repos,
        },
    )


def init():
    parser = argparse.ArgumentParser(
        description="Olah Huggingface Mirror Server."
    )
    parser.add_argument("--config", "-c", type=str, default="")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--hf-scheme", type=str, default="https", help="The scheme of huggingface site (http or https)")
    parser.add_argument("--hf-netloc", type=str, default="huggingface.co")
    parser.add_argument("--hf-lfs-netloc", type=str, default="cdn-lfs.huggingface.co")
    parser.add_argument("--mirror-scheme", type=str, default="http", help="The scheme of mirror site (http or https)")
    parser.add_argument("--mirror-netloc", type=str, default="localhost:8090")
    parser.add_argument("--mirror-lfs-netloc", type=str, default="localhost:8090")
    parser.add_argument("--has-lfs-site", action="store_true")
    parser.add_argument("--ssl-key", type=str, default=None, help="The SSL key file path, if HTTPS is used")
    parser.add_argument("--ssl-cert", type=str, default=None, help="The SSL cert file path, if HTTPS is used")
    parser.add_argument("--repos-path", type=str, default="./repos", help="The folder to save cached repositories")
    parser.add_argument("--cache-size-limit", type=str, default="", help="The limit size of cache. (Example values: '100MB', '2GB', '500KB')")
    parser.add_argument("--cache-clean-strategy", type=str, default="LRU", help="The clean strategy of cache. ('LRU', 'FIFO', 'LARGE_FIRST')")
    parser.add_argument("--log-path", type=str, default="./logs", help="The folder to save logs")
    args = parser.parse_args()
    
    logger = build_logger("olah", "olah.log", logger_dir=args.log_path)
    
    def is_default_value(args, arg_name):
        if hasattr(args, arg_name):
            arg_value = getattr(args, arg_name)
            arg_default = parser.get_default(arg_name)
            return arg_value == arg_default
        return False

    if args.config != "":
        config = OlahConfig(args.config)
    else:
        config = OlahConfig()
        
        if not is_default_value(args, "host"):
            config.host = args.host
        if not is_default_value(args, "port"):
            config.port = args.port
        
        if not is_default_value(args, "ssl_key"):
            config.ssl_key = args.ssl_key
        if not is_default_value(args, "ssl_cert"):
            config.ssl_cert = args.ssl_cert
        
        if not is_default_value(args, "repos_path"):
            config.repos_path = args.repos_path
        if not is_default_value(args, "hf_scheme"):
            config.hf_scheme = args.hf_scheme
        if not is_default_value(args, "hf_netloc"):
            config.hf_netloc = args.hf_netloc
        if not is_default_value(args, "hf_lfs_netloc"):
            config.hf_lfs_netloc = args.hf_lfs_netloc
        if not is_default_value(args, "mirror_scheme"):
            config.mirror_scheme = args.mirror_scheme
        if not is_default_value(args, "mirror_netloc"):
            config.mirror_netloc = args.mirror_netloc
        if not is_default_value(args, "mirror_lfs_netloc"):
            config.mirror_lfs_netloc = args.mirror_lfs_netloc
        if not is_default_value(args, "cache_size_limit"):
            config.cache_size_limit = convert_to_bytes(args.cache_size_limit)
        if not is_default_value(args, "cache_clean_strategy"):
            config.cache_clean_strategy = args.cache_clean_strategy
        else:
            if not args.has_lfs_site and not is_default_value(args, "mirror_netloc"):
                config.mirror_lfs_netloc = args.mirror_netloc

    if is_default_value(args, "host"):
        args.host = config.host
    if is_default_value(args, "port"):
        args.port = config.port
    if is_default_value(args, "ssl_key"):
        args.ssl_key = config.ssl_key
    if is_default_value(args, "ssl_cert"):
        args.ssl_cert = config.ssl_cert
    if is_default_value(args, "repos_path"):
        args.repos_path = config.repos_path
    
    if is_default_value(args, "hf_scheme"):
        args.hf_scheme = config.hf_scheme
    if is_default_value(args, "hf_netloc"):
        args.hf_netloc = config.hf_netloc
    if is_default_value(args, "hf_lfs_netloc"):
        args.hf_lfs_netloc = config.hf_lfs_netloc
    if is_default_value(args, "mirror_scheme"):
        args.mirror_scheme = config.mirror_scheme
    if is_default_value(args, "mirror_netloc"):
        args.mirror_netloc = config.mirror_netloc
    if is_default_value(args, "mirror_lfs_netloc"):
        args.mirror_lfs_netloc = config.mirror_lfs_netloc
    
    if is_default_value(args, "cache_size_limit"):
        args.cache_size_limit = config.cache_size_limit
    if is_default_value(args, "cache_clean_strategy"):
        args.cache_clean_strategy = config.cache_clean_strategy

    # Post processing
    if "," in args.host:
        args.host = args.host.split(",")
    
    args.mirror_scheme = config.mirror_scheme = "http" if args.ssl_key is None else "https"

    print(args)
    # Warnings
    if config.cache_size_limit is not None:
        logger.info(f"""
======== WARNING ========
Due to the cache_size_limit parameter being set, Olah will periodically delete cache files.
Please ensure that the cache directory specified in repos_path '{config.repos_path}' is correct.
Incorrect settings may result in unintended file deletion and loss!!! !!!
=========================""")
        for i in range(10):
            time.sleep(0.2)
    
    # Init app settings
    app.app_settings = AppSettings(config=config)
    return args

def main():
    args = init()
    if __name__ == "__main__":
        import uvicorn
        uvicorn.run(
            "olah.server:app",
            host=args.host,
            port=args.port,
            log_level="info",
            reload=False,
            ssl_keyfile=args.ssl_key,
            ssl_certfile=args.ssl_cert
        )

def cli():
    args = init()
    import uvicorn
    uvicorn.run(
        "olah.server:app",
        host=args.host,
        port=args.port,
        log_level="info",
        reload=False,
        ssl_keyfile=args.ssl_key,
        ssl_certfile=args.ssl_cert
    )

if __name__ in ["olah.server", "__main__"]:
    main()
