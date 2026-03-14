# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import glob
import os
import re
import traceback
from typing import Literal

import httpx
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse

from olah.errors import error_repo_not_found
from olah.proxy.files import cdn_file_get_generator, file_get_generator
from olah.proxy.lfs import lfs_get_generator, lfs_head_generator
from olah.server_access import build_repo_ref, ensure_repo_visibility, parse_repo_ref, parse_resolve_repo_ref
from olah.server_mirror import load_local_mirror_payload
from olah.server_responses import build_streaming_response
from olah.server_upstream import resolve_requested_commit
from olah.utils.repo_utils import get_org_repo


router = APIRouter()


class _NullLogger:
    def warning(self, *args, **kwargs):
        return None


def _get_logger(app: FastAPI):
    return getattr(app.state, "logger", None) or _NullLogger()


def _repo_name_from_cache_path(repo_path: str) -> str:
    parts = [part for part in re.split(r"[\\/]+", repo_path) if part]
    if len(parts) < 2:
        return repo_path
    return get_org_repo(parts[-2], parts[-1])


async def file_head_common(
    app: FastAPI,
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    request: Request,
) -> Response:
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, request.headers.get("authorization", None))
    if access_error is not None:
        return access_error

    head = load_local_mirror_payload(
        app,
        repo_ref,
        lambda local_repo: local_repo.get_file_head(commit_hash=commit, path=file_path),
        _get_logger(app),
    )
    if head is not None:
        return Response(headers=head)

    try:
        resolved_commit, commit_error = await resolve_requested_commit(
            app,
            repo_ref,
            commit,
            request.headers.get("authorization", None),
            repo_visible=True,
            missing_commit_response="repo_not_found",
        )
        if commit_error is not None:
            return commit_error
        generator = await file_get_generator(
            app,
            repo_type,
            org,
            repo,
            resolved_commit.resolved,
            file_path=file_path,
            method="HEAD",
            request=request,
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


async def cdn_proxy_common(
    app: FastAPI,
    repo_type: str,
    org: str,
    repo: str,
    hash_file: str,
    request: Request,
    method: Literal["HEAD", "GET"],
) -> Response:
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, request.headers.get("authorization", None))
    if access_error is not None:
        return access_error

    try:
        generator = await cdn_file_get_generator(
            app,
            repo_type,
            org,
            repo,
            hash_file,
            method=method,
            request=request,
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        return Response(status_code=504)


async def file_get_common(
    app: FastAPI,
    repo_type: str,
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    request: Request,
) -> Response:
    repo_ref = build_repo_ref(repo_type, org, repo)
    access_error = await ensure_repo_visibility(app, repo_ref, request.headers.get("authorization", None))
    if access_error is not None:
        return access_error

    content_stream = load_local_mirror_payload(
        app,
        repo_ref,
        lambda local_repo: local_repo.get_file(commit_hash=commit, path=file_path),
        _get_logger(app),
    )
    if content_stream is not None:
        return StreamingResponse(content_stream)

    try:
        resolved_commit, commit_error = await resolve_requested_commit(
            app,
            repo_ref,
            commit,
            request.headers.get("authorization", None),
            repo_visible=True,
            missing_commit_response="repo_not_found",
        )
        if commit_error is not None:
            return commit_error
        generator = await file_get_generator(
            app,
            repo_type,
            org,
            repo,
            resolved_commit.resolved,
            file_path=file_path,
            method="GET",
            request=request,
        )
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        traceback.print_exc()
        return Response(status_code=504)


async def lfs_proxy_common(
    app: FastAPI,
    dir1: str,
    dir2: str,
    hash_repo: str,
    hash_file: str,
    request: Request,
    method: Literal["HEAD", "GET"],
) -> Response:
    try:
        if method == "HEAD":
            generator = await lfs_head_generator(app, dir1, dir2, hash_repo, hash_file, request)
        else:
            generator = await lfs_get_generator(app, dir1, dir2, hash_repo, hash_file, request)
        return await build_streaming_response(generator)
    except httpx.ConnectTimeout:
        return Response(status_code=504)


@router.head("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_head_expanded(
    repo_type: str, org: str, repo: str, commit: str, file_path: str, request: Request
):
    return await file_head_common(
        request.app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@router.head("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_head_resolve_default(
    org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request
):
    repo_ref = parse_resolve_repo_ref(org_or_repo_type, repo_name)
    if repo_ref is None:
        return error_repo_not_found()
    return await file_head_common(
        request.app,
        repo_type=repo_ref.repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@router.head("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_head_compact(org_repo: str, commit: str, file_path: str, request: Request):
    repo_ref = parse_repo_ref("models", org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await file_head_common(
        request.app,
        repo_type=repo_ref.repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@router.head("/{org_repo}/{hash_file}")
@router.head("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_head(org_repo: str, hash_file: str, request: Request, repo_type: str = "models"):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await cdn_proxy_common(
        request.app,
        repo_type=repo_ref.repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        hash_file=hash_file,
        request=request,
        method="HEAD",
    )


@router.get("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_get_expanded(
    org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str
):
    return await file_get_common(
        request.app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@router.get("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_get_resolve_default(
    org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request
):
    repo_ref = parse_resolve_repo_ref(org_or_repo_type, repo_name)
    if repo_ref is None:
        return error_repo_not_found()
    return await file_get_common(
        request.app,
        repo_type=repo_ref.repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@router.get("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_get_compact(org_repo: str, commit: str, file_path: str, request: Request):
    repo_ref = parse_repo_ref("models", org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await file_get_common(
        request.app,
        repo_type=repo_ref.repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        commit=commit,
        file_path=file_path,
        request=request,
    )


@router.get("/{org_repo}/{hash_file}")
@router.get("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_get(
    org_repo: str, hash_file: str, request: Request, repo_type: str = "models"
):
    repo_ref = parse_repo_ref(repo_type, org_repo)
    if repo_ref is None:
        return error_repo_not_found()
    return await cdn_proxy_common(
        request.app,
        repo_type=repo_ref.repo_type,
        org=repo_ref.org,
        repo=repo_ref.repo,
        hash_file=hash_file,
        request=request,
        method="GET",
    )


@router.head("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_head(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    return await lfs_proxy_common(request.app, dir1, dir2, hash_repo, hash_file, request, method="HEAD")


@router.get("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_get(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    return await lfs_proxy_common(request.app, dir1, dir2, hash_repo, hash_file, request, method="GET")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return request.app.state.templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "scheme": request.app.state.app_settings.config.mirror_scheme,
            "netloc": request.app.state.app_settings.config.mirror_netloc,
        },
    )


@router.get("/repos", response_class=HTMLResponse)
async def repos(request: Request):
    datasets_repos = glob.glob(os.path.join(request.app.state.app_settings.config.repos_path, "api/datasets/*/*"))
    models_repos = glob.glob(os.path.join(request.app.state.app_settings.config.repos_path, "api/models/*/*"))
    spaces_repos = glob.glob(os.path.join(request.app.state.app_settings.config.repos_path, "api/spaces/*/*"))
    datasets_repos = [_repo_name_from_cache_path(repo) for repo in datasets_repos]
    models_repos = [_repo_name_from_cache_path(repo) for repo in models_repos]
    spaces_repos = [_repo_name_from_cache_path(repo) for repo in spaces_repos]

    return request.app.state.templates.TemplateResponse(
        "repos.html",
        {
            "request": request,
            "datasets_repos": datasets_repos,
            "models_repos": models_repos,
            "spaces_repos": spaces_repos,
        },
    )
