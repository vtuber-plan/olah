# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import hashlib
import logging
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_SHA256_HEX_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def normalize_sha256(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not _SHA256_HEX_RE.fullmatch(normalized):
        return None
    return normalized


def lfs_sha256_from_pathsinfo(pathinfo: Dict[str, Any]) -> Optional[str]:
    """Return LFS blob SHA256 from a Hugging Face paths-info row, if present."""
    lfs = pathinfo.get("lfs")
    if not isinstance(lfs, dict):
        return None
    return normalize_sha256(lfs.get("oid"))


async def verify_cache_sha256(
    cache_file,
    expected_sha256: str,
    *,
    save_path: Optional[str] = None,
) -> bool:
    """
    Compare assembled cache content against the expected LFS SHA256.
    On mismatch, invalidate all cached blocks so the next request refetches.
    """
    expected = normalize_sha256(expected_sha256)
    if expected is None:
        return True
    if not cache_file.is_complete():
        return True

    actual = await cache_file.content_sha256()
    if actual == expected:
        logger.info(
            "Cache SHA256 verified for %s",
            save_path or cache_file.path,
        )
        return True

    logger.error(
        "Cache SHA256 mismatch for %s: expected=%s actual=%s — invalidating blocks",
        save_path or cache_file.path,
        expected,
        actual,
    )
    cache_file.invalidate_all_blocks(
        f"sha256 mismatch expected {expected} got {actual}"
    )
    return False
