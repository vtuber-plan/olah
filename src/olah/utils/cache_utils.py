# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.


import json
import os
from typing import Dict, Mapping, Union

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
    observes a truncated file.
    """
    if not isinstance(headers, dict):
        headers = {k.lower(): v for k, v in headers.items()}
    rq = {"status_code": status_code, "headers": headers}
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


def _touch(path: str) -> None:
    """Best-effort bump of a file's mtime (LRU access marker)."""
    try:
        os.utime(path, None)
    except OSError:
        pass
