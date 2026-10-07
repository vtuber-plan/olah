# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""When the Hub last confirmed that a caller (``auth_utils.token_hash``) may see a repository."""

import os
import time
from typing import Optional

from olah.utils.file_utils import atomic_write_text


def _record_path(repos_path: str, repo_type: str, org: Optional[str], repo: str, caller: str) -> str:
    return os.path.join(repos_path, "access", repo_type, org or "", repo, caller)


def record_access(repos_path: str, repo_type: str, org: Optional[str], repo: str, caller: str) -> None:
    atomic_write_text(_record_path(repos_path, repo_type, org, repo, caller), repr(time.time()))


def access_age(repos_path: str, repo_type: str, org: Optional[str], repo: str, caller: str) -> Optional[float]:
    """Seconds since the Hub last confirmed this caller's access; None if unknown or future-dated."""
    try:
        with open(_record_path(repos_path, repo_type, org, repo, caller), "r", encoding="utf-8") as f:
            confirmed_at = float(f.read())
    except (OSError, ValueError):
        return None
    age = time.time() - confirmed_at
    return age if age >= 0 else None
