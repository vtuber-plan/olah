# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from fastapi import APIRouter

from olah.server_api_routes import (
    commits_proxy_common,
    meta_proxy_common,
    pathsinfo_proxy_common,
    router as api_router,
    tree_proxy_common,
)
from olah.server_file_routes import (
    cdn_proxy_common,
    file_get_common,
    file_head_common,
    lfs_proxy_common,
    router as file_router,
)


router = APIRouter()
router.include_router(api_router)
router.include_router(file_router)

