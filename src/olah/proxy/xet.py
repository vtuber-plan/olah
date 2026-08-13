# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Xet direct-link content route.

HF serves large files via Xet: the resolve 302 points at a signed, time-limited
CloudFront URL ``https://us.aws.cdn.hf.co/xet-bridge-{region}/{repo_hash}/{xet_hash}``.
This route exposes a STABLE olah URL ``/xet-bridge-{region}/{repo_hash}/{xet_hash}``
(no signature) and, on a cache miss, re-resolves a fresh signed URL via the
repo/file recorded in the Xet registry, then streams + caches the bytes
content-addressed by ``xet_hash``. Authorization reuses the LFS machinery.
"""

import os
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, Request

from olah.cache.olah_cache import OlahCache
from olah.constants import WORKER_API_TIMEOUT
from olah.proxy.files import _build_file_response
from olah.proxy.result import ProxyResult, single_chunk_body
from olah.utils.file_utils import make_dirs
from olah.utils.lfs_object_index import authorize_xet_object, get_xet_metadata
from olah.utils.repo_utils import get_org_repo
from olah.utils.rule_utils import check_cache_rules_hf


def _int_or_none(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _xet_resolve_url(
    app: FastAPI,
    repo_type: str,
    org: Optional[str],
    repo: str,
    file_path: str,
    commit: Optional[str],
    authorization: Optional[str],
) -> Tuple[Optional[str], Optional[int]]:
    """Re-resolve a fresh signed xet download URL + size for the file.

    The signed URL from HF expires, so it is never stored; we fetch a new one per
    cache miss. Returns (signed_url, size) or (None, None).
    """
    cfg = app.state.app_settings.config
    org_repo = get_org_repo(org, repo)
    prefix = "" if repo_type == "models" else f"{repo_type}/"
    rev = commit or "main"
    url = f"{cfg.hf_url_base()}/{prefix}{org_repo}/resolve/{rev}/{file_path}"
    headers = {}
    if authorization:
        headers["authorization"] = authorization
    try:
        async with httpx.AsyncClient(follow_redirects=False) as client:
            resp = await client.head(url, headers=headers, timeout=WORKER_API_TIMEOUT)
    except httpx.HTTPError:
        return None, None
    if resp.status_code in (301, 302, 303, 307, 308):
        size = _int_or_none(
            resp.headers.get("x-linked-size") or resp.headers.get("content-length")
        )
        return resp.headers.get("location"), size
    if resp.status_code == 200:
        return url, _int_or_none(resp.headers.get("content-length"))
    return None, None


async def xet_get_generator(
    app: FastAPI, xet_hash: str, request: Request, method: str
) -> ProxyResult:
    authorization = request.headers.get("authorization")
    refs, size, _lfs_oid = await get_xet_metadata(app, xet_hash)

    if not refs:
        # Unknown object: cannot re-resolve without a repo/file ref.
        return ProxyResult(
            status_code=404,
            headers={"x-error-message": "Unknown Xet object"},
            body=single_chunk_body(b""),
        )
    auth_status = await authorize_xet_object(app, xet_hash, authorization)
    if auth_status is not None:
        return ProxyResult(
            status_code=auth_status,
            headers={"x-error-message": "Forbidden"},
            body=single_chunk_body(b""),
        )

    # Caching permitted if any candidate repo matches the cache rules.
    allow_cache = False
    for ref in refs:
        repo_type, org, repo = ref[0], ref[1], ref[2]
        if await check_cache_rules_hf(app, repo_type, org, repo):
            allow_cache = True
            break

    repo_type, org, repo, file_path, commit = refs[0][:5]

    # HEAD answers from metadata only (no upstream fetch, no signed-URL resolve).
    if method.upper() == "HEAD":
        headers = {
            "etag": xet_hash,
            "accept-ranges": "bytes",
        }
        if size is not None:
            headers["content-length"] = str(size)
        if commit:
            from olah.constants import HUGGINGFACE_HEADER_X_REPO_COMMIT

            headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
        return ProxyResult(status_code=200, headers=headers, body=single_chunk_body(b""))

    repos_path = app.state.app_settings.config.repos_path
    save_path = os.path.join(repos_path, "lfs", "files", "xet", xet_hash)
    head_path = os.path.join(repos_path, "lfs", "heads", "xet", xet_hash)
    make_dirs(save_path)
    make_dirs(head_path)

    request_headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    # The signed CDN URL is self-authorizing; drop the bearer token so the CDN
    # doesn't reject an unexpected Authorization header.
    request_headers.pop("authorization", None)

    # Cache-first: if the object is already fully cached, serve directly from
    # disk with NO call to HF. This makes cache hits work offline and avoids a
    # resolve HEAD on every request. Only re-resolve a fresh signed URL when a
    # block is actually missing.
    if size and os.path.exists(os.path.join(save_path, "meta.bin")):
        peek = OlahCache(save_path, file_size=size, expected_etag=xet_hash)
        try:
            fully_cached = peek.is_fully_cached()
        finally:
            peek.close()
        if fully_cached:
            return await _build_file_response(
                app=app,
                save_path=save_path,
                head_path=head_path,
                request_headers=request_headers,
                method=method,
                upstream_url=None,
                file_size=size,
                etag=xet_hash,
                allow_cache=False,
                commit=commit,
            )

    # Cache miss (or unknown size): re-resolve a fresh signed URL, then fetch +
    # cache the missing blocks via the shared path.
    signed_url, resolved_size = await _xet_resolve_url(
        app, repo_type, org, repo, file_path, commit, authorization
    )
    file_size = resolved_size if resolved_size is not None else size
    if not signed_url or file_size is None:
        return ProxyResult(
            status_code=504,
            headers={"x-error-message": "Proxy Timeout"},
            body=single_chunk_body(b""),
        )

    return await _build_file_response(
        app=app,
        save_path=save_path,
        head_path=head_path,
        request_headers=request_headers,
        method=method,
        upstream_url=signed_url,
        file_size=file_size,
        etag=xet_hash,
        allow_cache=allow_cache,
        commit=commit,
    )
