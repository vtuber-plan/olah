# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from contextlib import asynccontextmanager
import os
import argparse
import traceback
from typing import Annotated, Optional, Union
from urllib.parse import urljoin
from fastapi import FastAPI, Header, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, Response, JSONResponse
from fastapi_utils.tasks import repeat_every
import git
import httpx
from pydantic import BaseSettings
from olah.configs import OlahConfig
from olah.errors import error_repo_not_found, error_page_not_found
from olah.mirror.repos import LocalMirrorRepo
from olah.proxy.files import cdn_file_get_generator, file_get_generator
from olah.proxy.lfs import lfs_get_generator, lfs_head_generator
from olah.proxy.meta import meta_generator, meta_proxy_cache
from olah.utils.url_utils import check_proxy_rules_hf, check_commit_hf, get_commit_hf, get_newest_commit_hf, parse_org_repo
from olah.constants import REPO_TYPES_MAPPING
from olah.utils.logging import build_logger

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


@repeat_every(seconds=60)
async def check_hf_connection() -> None:
    if app.app_settings.config.offline:
        return
    hf_online_status = await check_connection(
        "https://huggingface.co/datasets/Salesforce/wikitext/resolve/main/.gitattributes"
    )
    if not hf_online_status:
        logger.info(
            "Cannot reach Huggingface Official Site. Trying to connect hf-mirror."
        )
        hf_mirror_online_status = await check_connection(
            "https://hf-mirror.com/datasets/Salesforce/wikitext/resolve/main/.gitattributes"
        )
        if not hf_online_status and not hf_mirror_online_status:
            logger.error("Failed to reach Huggingface Official Site.")
            logger.error("Failed to reach hf-mirror Site.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await check_hf_connection()
    yield

# ======================
# Application
# ======================
app = FastAPI(lifespan=lifespan, debug=False)

class AppSettings(BaseSettings):
    # The address of the model controller.
    config: OlahConfig = OlahConfig()
    repos_path: str = "./repos"

# ======================
# API Hooks
# ======================
async def meta_proxy_common(repo_type: str, org: str, repo: str, commit: str, request: Request) -> Response:
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
        if not app.app_settings.config.offline and not await check_commit_hf(
            app, repo_type, org, repo, commit
        ):
            return error_repo_not_found()
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
        if commit_sha is None:
            return error_repo_not_found()
        # if branch name and online mode, refresh branch info
        if not app.app_settings.config.offline and commit_sha != commit:
            await meta_proxy_cache(app, repo_type, org, repo, commit, request)

        generator = meta_generator(app, repo_type, org, repo, commit_sha, request)
        headers = await generator.__anext__()
        return StreamingResponse(generator, headers=headers)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@app.get("/api/{repo_type}/{org_repo}")
async def meta_proxy(repo_type: str, org_repo: str, request: Request):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()
    if not app.app_settings.config.offline:
        new_commit = await get_newest_commit_hf(app, repo_type, org, repo)
    else:
        new_commit = "main"
    return await meta_proxy_common(
        repo_type=repo_type, org=org, repo=repo, commit=new_commit, request=request
    )


@app.get("/api/{repo_type}/{org}/{repo}/revision/{commit}")
async def meta_proxy_commit2(
    repo_type: str, org: str, repo: str, commit: str, request: Request
):
    return await meta_proxy_common(
        repo_type=repo_type, org=org, repo=repo, commit=commit, request=request
    )


@app.get("/api/{repo_type}/{org_repo}/revision/{commit}")
async def meta_proxy_commit(repo_type: str, org_repo: str, commit: str, request: Request):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    return await meta_proxy_common(
        repo_type=repo_type, org=org, repo=repo, commit=commit, request=request
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
            app, repo_type, org, repo, commit
        ):
            return error_repo_not_found()
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
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
    if org_or_repo_type in ["models", "datasets", "spaces"]:
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
        if not app.app_settings.config.offline and not await check_commit_hf(app, repo_type, org, repo, commit):
            return error_repo_not_found()
        commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
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
async def file_get3(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str):
    return await file_get_common(
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )

@app.get("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_get2(org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request):
    if org_or_repo_type in ["models", "datasets", "spaces"]:
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
async def cdn_file_get(org_repo: str, hash_file: str, request: Request, repo_type: str = "models"):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return error_repo_not_found()

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return error_repo_not_found()
    try:
        generator = await cdn_file_get_generator(app, repo_type, org, repo, hash_file, method="GET", request=request)
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
async def index():
    with open(os.path.join(os.path.dirname(__file__), "../static/index.html"), "r", encoding="utf-8") as f:
        page = f.read()
    return page

if __name__ in ["__main__", "olah.server"]:
    parser = argparse.ArgumentParser(
        description="Olah Huggingface Mirror Server."
    )
    parser.add_argument("--config", "-c", type=str, default="")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--ssl-key", type=str, default=None, help="The SSL key file path, if HTTPS is used")
    parser.add_argument("--ssl-cert", type=str, default=None, help="The SSL cert file path, if HTTPS is used")
    parser.add_argument("--repos-path", type=str, default="./repos", help="The folder to save cached repositories")
    parser.add_argument("--log-path", type=str, default="./logs", help="The folder to save logs")
    args = parser.parse_args()
    print(args)

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

    app.app_settings = AppSettings(
        config=config,
        repos_path=args.repos_path,
    )

    import uvicorn
    if __name__ == "__main__":
        uvicorn.run(
            "olah.server:app",
            host=args.host,
            port=args.port,
            log_level="info",
            reload=False,
            ssl_keyfile=args.ssl_key,
            ssl_certfile=args.ssl_cert
        )
