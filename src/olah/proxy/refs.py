"""Mutable repository refs, kept separately from immutable revision caches."""

import hashlib
import json
import os
from typing import Optional
from urllib.parse import urljoin

import httpx

from olah.constants import WORKER_API_TIMEOUT
from olah.errors import error_proxy_invalid_data
from olah.proxy.result import ProxyResult, single_chunk_body
from olah.utils.cache_utils import read_cache_request, write_cache_request
from olah.utils.repo_utils import get_org_repo
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.upstream_fallback import is_offline


def _valid_refs(content: bytes, include_prs: bool) -> bool:
    try:
        payload = json.loads(content)
    except (ValueError, UnicodeDecodeError):
        return False
    groups = ["branches", "tags", "converts"]
    if include_prs:
        groups.append("pullRequests")
    return isinstance(payload, dict) and all(
        isinstance(payload.get(group), list)
        and all(
            isinstance(ref, dict)
            and all(isinstance(ref.get(key), str) for key in ("name", "ref", "targetCommit"))
            for ref in payload[group]
        )
        for group in groups
    )


async def refs_generator(
    app,
    repo_type: str,
    org: Optional[str],
    repo: str,
    include_prs: bool,
    authorization: Optional[str],
) -> ProxyResult:
    config = app.state.app_settings.config
    org_repo = get_org_repo(org, repo)
    # An authenticated refs response can contain refs invisible to anonymous
    # callers. Do not share it between credentials, or store the token itself.
    identity = "anonymous" if authorization is None else hashlib.sha256(authorization.encode()).hexdigest()
    save_path = os.path.join(
        config.repos_path,
        f"api/{repo_type}/{org_repo}/refs/{identity}/refs_include_prs_{include_prs}.json",
    )

    if is_offline(app):
        try:
            cached = await read_cache_request(save_path)
            if cached["status_code"] == 200 and _valid_refs(cached["content"], include_prs):
                return ProxyResult(200, cached["headers"], single_chunk_body(cached["content"]))
        except (OSError, ValueError, KeyError):
            pass
        return ProxyResult(
            404,
            {"x-error-code": "EntryNotFound", "x-error-message": "Repository refs are not cached locally"},
            single_chunk_body(b""),
        )

    # Refs can move. Always fetch their current value online; a failed refresh
    # must neither replay a stale success nor replace the last good snapshot.
    headers = {"authorization": authorization} if authorization is not None else {}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(
            urljoin(config.hf_url_base(), f"/api/{repo_type}/{org_repo}/refs"),
            params={"include_prs": 1} if include_prs else {},
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        )
    response_headers = dict(response.headers)
    # httpx.content is decoded, so wire-length/encoding headers no longer apply.
    for name in ("content-encoding", "content-length", "transfer-encoding", "set-cookie"):
        response_headers.pop(name, None)
    if response.status_code == 200:
        if not _valid_refs(response.content, include_prs):
            error = error_proxy_invalid_data()
            return ProxyResult(error.status_code, error.headers, single_chunk_body(error.body))
        if await check_cache_rules_hf(app, repo_type, org, repo):
            await write_cache_request(save_path, 200, response_headers, response.content)
    return ProxyResult(response.status_code, response_headers, single_chunk_body(response.content))
