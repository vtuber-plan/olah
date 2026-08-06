# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
from typing import AsyncIterator, Dict, Literal, Mapping, Optional
from urllib.parse import urljoin

import httpx
from fastapi import FastAPI

from olah.constants import WORKER_API_TIMEOUT

from olah.utils.api_proxy_utils import build_upstream_api_headers, normalize_api_response
from olah.utils.cache_utils import read_cache_request, write_cache_request
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.repo_utils import get_org_repo
from olah.utils.file_utils import make_dirs
from olah.proxy.result import ProxyResult, single_chunk_body


def build_hf_tree_url(
    base_url: str,
    repo_type: Literal["models", "datasets", "spaces"],
    org_repo: str,
    commit: str,
    path: str,
) -> str:
    path = path.strip("/")
    if path:
        rel = f"/api/{repo_type}/{org_repo}/tree/{commit}/{path}"
    else:
        rel = f"/api/{repo_type}/{org_repo}/tree/{commit}"
    return urljoin(base_url, rel)


async def _tree_cache_generator(save_path: str) -> ProxyResult:
    cache_rq = await read_cache_request(save_path)
    content, headers = normalize_api_response(cache_rq["content"], cache_rq["headers"])
    return ProxyResult(
        status_code=cache_rq["status_code"],
        headers=headers,
        body=single_chunk_body(content),
    )

async def _tree_proxy_generator(
    app: FastAPI,
    headers: Dict[str, str],
    tree_url: str,
    method: str,
    params: Mapping[str, str],
    allow_cache: bool,
    save_path: str,
) -> ProxyResult:
    response_status_code = 500
    response_headers: Dict[str, str] = {}
    content = b""

    async with httpx.AsyncClient(follow_redirects=True) as client:
        async with client.stream(
            method=method,
            url=tree_url,
            params=params,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        ) as response:
            response_status_code = response.status_code
            response_headers = dict(response.headers)
            content = await response.aread()

    content, response_headers = normalize_api_response(content, response_headers)

    async def body_iter() -> AsyncIterator[bytes]:
        yield content
        if allow_cache and response_status_code == 200:
            make_dirs(save_path)
            await write_cache_request(
                save_path, response_status_code, response_headers, content
            )

    return ProxyResult(
        status_code=response_status_code,
        headers=response_headers,
        body=body_iter(),
    )


async def tree_generator(
    app: FastAPI,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    commit: str,
    path: str,
    recursive: bool,
    expand: bool,
    override_cache: bool,
    method: str,
    authorization: Optional[str],
) -> ProxyResult:
    headers = build_upstream_api_headers(authorization)

    org_repo = get_org_repo(org, repo)
    # save
    repos_path = app.state.app_settings.config.repos_path
    save_dir = os.path.join(
        repos_path, f"api/{repo_type}/{org_repo}/tree/{commit}/{path}"
    )
    save_path = os.path.join(save_dir, f"tree_{method}_recursive_{recursive}_expand_{expand}.json")

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    tree_url = build_hf_tree_url(
        app.state.app_settings.config.hf_url_base(),
        repo_type,
        org_repo,
        commit,
        path,
    )
    # proxy
    if use_cache and not override_cache:
        return await _tree_cache_generator(save_path)
    return await _tree_proxy_generator(
        app, headers, tree_url, method, {"recursive": recursive, "expand": expand}, allow_cache, save_path
    )
