
from typing import Literal

import httpx
from olah.configs import OlahConfig
from olah.constants import WORKER_API_TIMEOUT

async def check_commit_hf(app, repo_type: Literal["model", "dataset"], org: str, repo: str, commit: str) -> bool:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{app.app_settings.hf_url}/api/{repo_type}s/{org}/{repo}/revision/{commit}",
                   timeout=WORKER_API_TIMEOUT)
    return response.status_code == 200

async def check_proxy_rules_hf(app, repo_type: Literal["model", "dataset"], org: str, repo: str) -> bool:
    config: OlahConfig = app.app_settings.config
    return config.proxy.allow(f"{org}/{repo}")

async def check_cache_rules_hf(app, repo_type: Literal["model", "dataset"], org: str, repo: str) -> bool:
    config: OlahConfig = app.app_settings.config
    return config.cache.allow(f"{org}/{repo}")
