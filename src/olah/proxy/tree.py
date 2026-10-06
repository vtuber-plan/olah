# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
from typing import Literal, Optional
from urllib.parse import urljoin
from fastapi import FastAPI

from olah.errors import error_entry_not_found

from olah.utils.cache_utils import read_cache_request
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.repo_utils import get_org_repo
from olah.proxy.api_proxy import proxy_api_request
from olah.proxy.result import ProxyResult, single_chunk_body


async def _tree_cache_generator(save_path: str) -> ProxyResult:
    cache_rq = await read_cache_request(save_path)
    return ProxyResult(
        status_code=cache_rq["status_code"],
        headers=cache_rq["headers"],
        body=single_chunk_body(cache_rq["content"]),
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
    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization

    org_repo = get_org_repo(org, repo)
    # save
    repos_path = app.state.app_settings.config.repos_path
    save_dir = os.path.join(
        repos_path, f"api/{repo_type}/{org_repo}/tree/{commit}/{path}"
    )
    save_path = os.path.join(save_dir, f"tree_{method}_recursive_{recursive}_expand_{expand}.json")

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    org_repo = get_org_repo(org, repo)
    tree_url = urljoin(
        app.state.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/tree/{commit}/{path}",
    )
    # proxy
    offline = app.state.app_settings.config.offline
    if use_cache and (offline or not override_cache):
        return await _tree_cache_generator(save_path)
    if offline:
        missing = error_entry_not_found()
        return ProxyResult(
            status_code=missing.status_code,
            headers=missing.headers,
            body=single_chunk_body(missing.body),
        )
    return await proxy_api_request(
        tree_url, method, headers, allow_cache, save_path, params={"recursive": recursive, "expand": expand}
    )
