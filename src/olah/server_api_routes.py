# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import traceback
from typing import Annotated, List, Literal, Optional
from urllib.parse import urljoin

import httpx
from fastapi import APIRouter, FastAPI, Form, Request
from fastapi.responses import JSONResponse, Response

from olah.errors import error_repo_not_found
from olah.proxy.commits import commits_generator
from olah.proxy.meta import meta_generator
from olah.proxy.pathsinfo import pathsinfo_generator
from olah.proxy.tree import tree_generator
from olah.server_access import build_repo_ref, ensure_repo_visibility, parse_repo_ref
from olah.server_mirror import load_local_mirror_payload
from olah.server_responses import build_streaming_response
from olah.server_upstream import get_latest_commit, prepare_revision_generator, resolve_requested_commit
from olah.utils.url_utils import clean_path


router = APIRouter()


class _NullLogger:
    def warning(self, *args, **kwargs):
        return None


def _get_logger(app: FastAPI):
    return getattr(app.state, "logger", None) or _NullLogger()


async def meta_proxy_common(
    app: FastAPI,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    commit: str,
    method: str,
    authorization: Optional[str],
) -> Response:
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, authorization)
    if access_error is not None:
        return access_error

    meta_data = load_local_mirror_payload(
        app,
        repo_ref,
        lambda local_repo: local_repo.get_meta(commit),
        _get_logger(app),
    )
    if meta_data is not None:
        return JSONResponse(content=meta_data)

    try:
        resolved_commit, commit_error = await resolve_requested_commit(
            app,
            repo_ref,
            commit,
            authorization,
            repo_visible=True,
            missing_commit_response="revision_not_found",
        )
        if commit_error is not None:
            return commit_error

        generator = await prepare_revision_generator(
            app,
            resolved_commit,
            lambda target_commit, override_cache: meta_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=target_commit,
                override_cache=override_cache,
                method=method,
                authorization=authorization,
            ),
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


async def tree_proxy_common(
    app: FastAPI,
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    path: str,
    recursive: bool,
    expand: bool,
    method: str,
    authorization: Optional[str],
) -> Response:
    path = clean_path(path)
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, authorization)
    if access_error is not None:
        return access_error

    tree_data = load_local_mirror_payload(
        app,
        repo_ref,
        lambda local_repo: local_repo.get_tree(commit, path, recursive=recursive, expand=expand),
        _get_logger(app),
    )
    if tree_data is not None:
        return JSONResponse(content=tree_data)

    try:
        resolved_commit, commit_error = await resolve_requested_commit(
            app,
            repo_ref,
            commit,
            authorization,
            repo_visible=True,
            missing_commit_response="revision_not_found",
        )
        if commit_error is not None:
            return commit_error

        generator = await prepare_revision_generator(
            app,
            resolved_commit,
            lambda target_commit, override_cache: tree_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=target_commit,
                path=path,
                recursive=recursive,
                expand=expand,
                override_cache=override_cache,
                method=method,
                authorization=authorization,
            ),
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


async def pathsinfo_proxy_common(
    app: FastAPI,
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    paths: List[str],
    method: str,
    authorization: Optional[str],
) -> Response:
    paths = [clean_path(path) for path in paths]
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, authorization)
    if access_error is not None:
        return access_error

    pathsinfo_data = load_local_mirror_payload(
        app,
        repo_ref,
        lambda local_repo: local_repo.get_pathinfos(commit, paths),
        _get_logger(app),
    )
    if pathsinfo_data is not None:
        return JSONResponse(content=pathsinfo_data)

    try:
        resolved_commit, commit_error = await resolve_requested_commit(
            app,
            repo_ref,
            commit,
            authorization,
            repo_visible=True,
            missing_commit_response="revision_not_found",
        )
        if commit_error is not None:
            return commit_error

        generator = await prepare_revision_generator(
            app,
            resolved_commit,
            lambda target_commit, override_cache: pathsinfo_generator(
                app,
                repo_type,
                org,
                repo,
                target_commit,
                paths,
                override_cache=override_cache,
                method=method,
                authorization=authorization,
            ),
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


async def commits_proxy_common(
    app: FastAPI,
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    method: str,
    authorization: Optional[str],
) -> Response:
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, authorization)
    if access_error is not None:
        return access_error

    commits_data = load_local_mirror_payload(
        app,
        repo_ref,
        lambda local_repo: local_repo.get_commits(commit),
        _get_logger(app),
    )
    if commits_data is not None:
        return JSONResponse(content=commits_data)

    try:
        resolved_commit, commit_error = await resolve_requested_commit(
            app,
            repo_ref,
            commit,
            authorization,
            repo_visible=True,
            missing_commit_response="revision_not_found",
        )
        if commit_error is not None:
            return commit_error

        generator = await prepare_revision_generator(
            app,
            resolved_commit,
            lambda target_commit, override_cache: commits_generator(
                app=app,
                repo_type=repo_type,
                org=org,
                repo=repo,
                commit=target_commit,
                override_cache=override_cache,
                method=method,
                authorization=authorization,
            ),
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


@router.get("/api/whoami-v2")
async def whoami_v2(request: Request):
    new_headers = {k.lower(): v for k, v in request.headers.items()}
    new_headers["host"] = request.app.state.app_settings.config.hf_netloc
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method="GET",
            url=urljoin(request.app.state.app_settings.config.hf_url_base(), "/api/whoami-v2"),
            headers=new_headers,
            timeout=10,
        )
    response_headers = {k.lower(): v for k, v in response.headers.items()}
    response_headers.pop("content-encoding", None)
    response_headers.pop("content-length", None)
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=response_headers,
    )


@router.head("/api/{repo_type}/{org_repo}")
@router.get("/api/{repo_type}/{org_repo}")
async def meta_proxy_compact(repo_type: str, org_repo: str, request: Request):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    new_commit = await get_latest_commit(request.app, repo_ref, request.headers.get("authorization", None))
    if new_commit is None:
        return error_repo_not_found()
    return await meta_proxy_common(
        request.app,
        repo_type=repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=new_commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org}/{repo}")
@router.get("/api/{repo_type}/{org}/{repo}")
async def meta_proxy_expanded(repo_type: str, org: str, repo: str, request: Request):
    repo_ref = build_repo_ref(repo_type, org, repo)
    new_commit = await get_latest_commit(request.app, repo_ref, request.headers.get("authorization", None))
    if new_commit is None:
        return error_repo_not_found()
    return await meta_proxy_common(
        request.app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=new_commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org}/{repo}/revision/{commit}")
@router.get("/api/{repo_type}/{org}/{repo}/revision/{commit}")
async def meta_proxy_commit_expanded(
    repo_type: str, org: str, repo: str, commit: str, request: Request
):
    return await meta_proxy_common(
        request.app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org_repo}/revision/{commit}")
@router.get("/api/{repo_type}/{org_repo}/revision/{commit}")
async def meta_proxy_commit_compact(
    repo_type: str, org_repo: str, commit: str, request: Request
):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await meta_proxy_common(
        request.app,
        repo_type=repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org}/{repo}/tree/{commit}/{file_path:path}")
@router.get("/api/{repo_type}/{org}/{repo}/tree/{commit}/{file_path:path}")
async def tree_proxy_commit_expanded(
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    request: Request,
    recursive: bool = False,
    expand: bool = False,
):
    return await tree_proxy_common(
        request.app,
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


@router.head("/api/{repo_type}/{org_repo}/tree/{commit}/{file_path:path}")
@router.get("/api/{repo_type}/{org_repo}/tree/{commit}/{file_path:path}")
async def tree_proxy_commit_compact(
    repo_type: str,
    org_repo: str,
    commit: str,
    file_path: str,
    request: Request,
    recursive: bool = False,
    expand: bool = False,
):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await tree_proxy_common(
        request.app,
        repo_type=repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        path=file_path,
        recursive=recursive,
        expand=expand,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org}/{repo}/paths-info/{commit}")
@router.post("/api/{repo_type}/{org}/{repo}/paths-info/{commit}")
async def pathsinfo_proxy_commit_expanded(
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    paths: Annotated[List[str], Form()],
    request: Request,
):
    return await pathsinfo_proxy_common(
        request.app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        paths=paths,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org_repo}/paths-info/{commit}")
@router.post("/api/{repo_type}/{org_repo}/paths-info/{commit}")
async def pathsinfo_proxy_commit_compact(
    repo_type: str,
    org_repo: str,
    commit: str,
    paths: Annotated[List[str], Form()],
    request: Request,
):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await pathsinfo_proxy_common(
        request.app,
        repo_type=repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        paths=paths,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org}/{repo}/commits/{commit}")
@router.get("/api/{repo_type}/{org}/{repo}/commits/{commit}")
async def commits_proxy_commit_expanded(
    repo_type: str, org: str, repo: str, commit: str, request: Request
):
    return await commits_proxy_common(
        request.app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )


@router.head("/api/{repo_type}/{org_repo}/commits/{commit}")
@router.get("/api/{repo_type}/{org_repo}/commits/{commit}")
async def commits_proxy_commit_compact(
    repo_type: str, org_repo: str, commit: str, request: Request
):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await commits_proxy_common(
        request.app,
        repo_type=repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        method=request.method.lower(),
        authorization=request.headers.get("authorization", None),
    )
