# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from typing import AsyncIterator, Dict, Mapping, Optional

import httpx

from olah.constants import WORKER_API_TIMEOUT
from olah.proxy.result import ProxyResult
from olah.utils.cache_utils import write_cache_request
from olah.utils.file_utils import make_dirs


async def proxy_api_request(
    url: str,
    method: str,
    headers: Dict[str, str],
    allow_cache: bool,
    save_path: str,
    params: Optional[Mapping[str, str]] = None,
) -> ProxyResult:
    """Stream a Hub API response from a single upstream request, caching a 200.

    The body is relayed raw (still content-encoded) so it stays consistent with
    the relayed headers and with the cache format readers expect.
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
    await body.__anext__()
    return ProxyResult(
        status_code=upstream["status_code"],
        headers=upstream["headers"],
        body=body,
    )
