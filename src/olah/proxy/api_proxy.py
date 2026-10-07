# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import logging
from typing import AsyncIterator, Dict, Mapping, Optional

import httpx

from olah.constants import WORKER_API_TIMEOUT
from olah.proxy.result import ProxyResult, single_chunk_body
from olah.utils.cache_utils import read_cache_request_if_fresh, write_cache_request
from olah.utils.file_utils import make_dirs

logger = logging.getLogger(__name__)


async def proxy_api_request(
    url: str,
    method: str,
    headers: Dict[str, str],
    allow_cache: bool,
    save_path: str,
    params: Optional[Mapping[str, str]] = None,
    max_stale: float = 0,
) -> ProxyResult:
    """Stream a Hub API response from a single upstream request, caching a 200.

    The body is relayed raw (still content-encoded) so it stays consistent with
    the relayed headers and with the cache format readers expect. When the Hub
    rate-limits or fails, a cached copy younger than ``max_stale`` is served instead.
    """
    upstream: Dict = {}

    async def body_iter() -> AsyncIterator[bytes]:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream(
                method=method,
                url=url,
                params=params,
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
            ) as response:
                upstream["status_code"] = response.status_code
                upstream["headers"] = dict(response.headers)
                yield b""
                chunks = []
                async for raw_chunk in response.aiter_raw():
                    if not raw_chunk:
                        continue
                    chunks.append(raw_chunk)
                    yield raw_chunk

        if allow_cache and upstream["status_code"] == 200:
            make_dirs(save_path)
            await write_cache_request(
                save_path, upstream["status_code"], upstream["headers"], b"".join(chunks)
            )

    body = body_iter()
    # Run up to the response headers. A started async generator is always
    # finalized (aclose on early exit or garbage collection), so the upstream
    # connection is released even if the caller never consumes the body.
    try:
        await body.__anext__()
    except httpx.HTTPError:
        stale = await _stale_copy(save_path, max_stale)
        if stale is None:
            raise
        return stale
    if upstream["status_code"] == 429 or upstream["status_code"] >= 500:
        stale = await _stale_copy(save_path, max_stale)
        if stale is not None:
            await body.aclose()
            return stale
    return ProxyResult(
        status_code=upstream["status_code"],
        headers=upstream["headers"],
        body=body,
    )


async def _stale_copy(save_path: str, max_stale: float) -> Optional[ProxyResult]:
    if max_stale <= 0:
        return None
    cached = await read_cache_request_if_fresh(save_path, max_stale)
    if cached is None:
        return None
    logger.warning("Upstream unavailable; serving cached %s", save_path)
    return ProxyResult(
        status_code=cached["status_code"],
        headers=cached["headers"],
        body=single_chunk_body(cached["content"]),
    )
