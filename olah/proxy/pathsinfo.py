# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import json
import os
from typing import AsyncGenerator, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import quote, urljoin
from fastapi import FastAPI, Request

import httpx
from olah.constants import CHUNK_SIZE, WORKER_API_TIMEOUT

from olah.utils.cache_utils import read_cache_request, write_cache_request
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.repo_utils import get_org_repo
from olah.utils.file_utils import make_dirs


async def _pathsinfo_cache(save_path: str) -> Tuple[int, Dict[str, str], bytes]:
    cache_rq = await read_cache_request(save_path)
    return cache_rq["status_code"], cache_rq["headers"], cache_rq["content"]


async def _pathsinfo_proxy(
    app: FastAPI,
    headers: Dict[str, str],
    pathsinfo_url: str,
    method: str,
    path: str,
    allow_cache: bool,
    save_path: str,
) -> Tuple[int, Dict[str, str], bytes]:
    headers = {k: v for k, v in headers.items()}
    if "content-length" in headers:
        headers.pop("content-length")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.request(
            method=method,
            url=pathsinfo_url,
            headers=headers,
            data={"paths": path},
            timeout=WORKER_API_TIMEOUT,
        )

        if allow_cache and response.status_code == 200:
            make_dirs(save_path)
            await write_cache_request(
                save_path,
                response.status_code,
                response.headers,
                bytes(response.content),
            )
    return response.status_code, response.headers, response.content


async def pathsinfo_generator(
    app: FastAPI,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    commit: str,
    paths: List[str],
    override_cache: bool,
    method: str,
    authorization: Optional[str],
) -> AsyncGenerator[Union[int, Dict[str, str], bytes], None]:
    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization

    # save
    repos_path = app.app_settings.config.repos_path

    final_content = []
    for path in paths:
        save_dir = os.path.join(
            repos_path, f"api/{repo_type}/{org}/{repo}/paths-info/{commit}/{path}"
        )

        save_path = os.path.join(save_dir, f"paths-info_{method}.json")

        use_cache = os.path.exists(save_path)
        allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

        org_repo = get_org_repo(org, repo)
        pathsinfo_url = urljoin(
            app.app_settings.config.hf_url_base(),
            f"/api/{repo_type}/{org_repo}/paths-info/{commit}",
        )
        # proxy
        if use_cache and not override_cache:
            status, headers, content = await _pathsinfo_cache(save_path)
        else:
            status, headers, content = await _pathsinfo_proxy(
                app, headers, pathsinfo_url, method, path, allow_cache, save_path
            )

        try:
            content_json = json.loads(content)
        except json.JSONDecodeError:
            continue
        if status == 200 and isinstance(content_json, list):
            final_content.extend(content_json)

    yield 200
    yield {'content-type': 'application/json'}
    yield json.dumps(final_content, ensure_ascii=True)
