# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.


import json
import os
import time
from typing import Dict, Mapping, Optional, Union

from olah.utils.file_utils import atomic_write_bytes, atomic_write_text


def _body_path(save_path: str) -> str:
    """Sidecar path for the raw response body, alongside the JSON metadata."""
    return save_path + ".body"


async def write_cache_request(
    save_path: str,
    status_code: int,
    headers: Union[Dict[str, str], Mapping],
    content: bytes,
) -> None:
    """Atomically persist a cached API response as JSON metadata + raw body.

    Metadata (status + headers) goes to ``save_path``; the raw response bytes go
    to a sibling ``<save_path>.body`` sidecar, stored verbatim rather than
    hex-encoded (the legacy inline-hex format doubled the on-disk size). Both
    files are written tmp+fsync+rename so a crash or a concurrent reader never
    observes a truncated file. The envelope records ``cached_at`` (wall-clock
    seconds) so metadata caches can be reused for a configurable TTL; the file
    mtime cannot serve that role because ``read_cache_request`` touches it as an
    LRU access marker.
    """
    if not isinstance(headers, dict):
        headers = {k.lower(): v for k, v in headers.items()}
    rq = {"status_code": status_code, "headers": headers, "cached_at": time.time()}
    atomic_write_text(save_path, json.dumps(rq, ensure_ascii=False))
    atomic_write_bytes(_body_path(save_path), bytes(content))


async def read_cache_request(save_path: str) -> Dict:
    """Load a cached API response -> ``{status_code, headers, content}``.

    Prefers the raw ``.body`` sidecar; falls back to the legacy inline-hex
    ``content`` field for caches written before the sidecar split. Bumps the
    files' mtimes as a reliable last-access signal for LRU eviction (FS atime is
    unreliable under noatime/relatime).
    """
    body_path = _body_path(save_path)

    with open(save_path, "r", encoding="utf-8") as f:
        rq = json.loads(f.read())

    if os.path.exists(body_path):
        with open(body_path, "rb") as f:
            rq["content"] = f.read()
        _touch(save_path)
        _touch(body_path)
        return rq

    # Legacy format: content hex-encoded inline in the JSON.
    rq["content"] = bytes.fromhex(rq["content"])
    _touch(save_path)
    return rq


def cache_age(save_path: str) -> Optional[float]:
    """Seconds since a cached response was written, or None if undeterminable.

    ``None`` covers a missing envelope and envelopes written before
    ``cached_at`` was recorded; both are treated as infinitely old by callers.
    """
    try:
        with open(save_path, "r", encoding="utf-8") as f:
            rq = json.loads(f.read())
    except (OSError, ValueError):
        return None
    cached_at = rq.get("cached_at") if isinstance(rq, dict) else None
    if not isinstance(cached_at, (int, float)):
        return None
    return max(0.0, time.time() - cached_at)


async def read_cache_request_if_fresh(save_path: str, ttl_seconds: float) -> Optional[Dict]:
    """Load a cached API response only when it is younger than ``ttl_seconds``.

    Returns the same dict as ``read_cache_request``, or ``None`` when the entry
    is missing, corrupt, undatable or older than the TTL (``ttl_seconds <= 0``
    always misses). Callers fall back to revalidation; a stale entry can still
    be served via plain ``read_cache_request`` when the upstream fails.
    """
    if ttl_seconds <= 0 or not os.path.exists(save_path):
        return None
    age = cache_age(save_path)
    if age is None or age > ttl_seconds:
        return None
    try:
        return await read_cache_request(save_path)
    except (OSError, ValueError, KeyError):
        return None


def _touch(path: str) -> None:
    """Best-effort bump of a file's mtime (LRU access marker)."""
    try:
        os.utime(path, None)
    except OSError:
        pass
