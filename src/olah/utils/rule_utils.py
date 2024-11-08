# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.


from typing import Dict, Literal, Optional, Tuple, Union
from olah.configs import OlahConfig
from .repo_utils import get_org_repo


async def check_proxy_rules_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
) -> bool:
    config: OlahConfig = app.app_settings.config
    org_repo = get_org_repo(org, repo)
    return config.proxy.allow(org_repo)


async def check_cache_rules_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
) -> bool:
    config: OlahConfig = app.app_settings.config
    org_repo = get_org_repo(org, repo)
    return config.cache.allow(org_repo)
