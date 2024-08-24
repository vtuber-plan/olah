# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
import shutil
import tempfile
from typing import Dict, Literal
from urllib.parse import urljoin
from fastapi import FastAPI, Request

import httpx
from olah.constants import CHUNK_SIZE, WORKER_API_TIMEOUT

from olah.utils.cache_utils import _read_cache_request, _write_cache_request
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.repo_utils import get_org_repo
from olah.utils.file_utils import make_dirs


async def _tree_cache_generator(save_path: str):
    cache_rq = await _read_cache_request(save_path)
    yield cache_rq["headers"]
    yield cache_rq["content"]


async def tree_proxy_cache(
    app: FastAPI,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    commit: str,
    path: str,
    request: Request,
):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    method = request.method.lower()
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(
        repos_path, f"api/{repo_type}/{org}/{repo}/tree/{commit}/{path}"
    )
    save_path = os.path.join(save_dir, f"tree_{method}.json")

    # url
    org_repo = get_org_repo(org, repo)
    tree_url = urljoin(
        app.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/tree/{commit}/{path}",
    )
    headers = {}
    if "authorization" in request.headers:
        headers["authorization"] = request.headers["authorization"]
    async with httpx.AsyncClient() as client:
        response = await client.request(
            method=request.method,
            url=tree_url,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
            follow_redirects=True,
        )
        if response.status_code == 200:
            make_dirs(save_path)
            await _write_cache_request(
                save_path, response.status_code, response.headers, response.content
            )
        elif response.status_code == 404:
            pass
        else:
            raise Exception(
                f"Cannot get the branch info from the url {tree_url}, status: {response.status_code}"
            )


async def _tree_proxy_generator(
    app: FastAPI,
    headers: Dict[str, str],
    tree_url: str,
    allow_cache: bool,
    method: str,
    save_path: str,
):
    async with httpx.AsyncClient(follow_redirects=True) as client:
        content_chunks = []
        async with client.stream(
            method=method,
            url=tree_url,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        ) as response:
            response_status_code = response.status_code
            response_headers = response.headers
            yield response_headers

            async for raw_chunk in response.aiter_raw():
                if not raw_chunk:
                    continue
                content_chunks.append(raw_chunk)
                yield raw_chunk

        content = bytearray()
        for chunk in content_chunks:
            content += chunk

        if response_status_code == 200:
            make_dirs(save_path)
            await _write_cache_request(
                save_path, response_status_code, response_headers, bytes(content)
            )


async def tree_generator(
    app: FastAPI,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    commit: str,
    path: str,
    request: Request,
):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    method = request.method.lower()
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(
        repos_path, f"api/{repo_type}/{org}/{repo}/tree/{commit}/{path}"
    )
    save_path = os.path.join(save_dir, f"tree_{method}.json")

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    org_repo = get_org_repo(org, repo)
    tree_url = urljoin(
        app.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/tree/{commit}/{path}",
    )
    # proxy
    if use_cache:
        async for item in _tree_cache_generator(save_path):
            yield item
    else:
        async for item in _tree_proxy_generator(
            app, headers, tree_url, allow_cache, method, save_path
        ):
            yield item
