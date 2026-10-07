# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import hashlib
from typing import Optional


def token_hash(authorization: Optional[str]) -> str:
    """Non-reversible key for a caller's credentials ("anon" without any)."""
    if not authorization:
        return "anon"
    return hashlib.sha256(authorization.encode("utf-8")).hexdigest()
