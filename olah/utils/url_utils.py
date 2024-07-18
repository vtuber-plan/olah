# coding=utf-8
# Copyright 2024 XiaHan
# 
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import datetime
import os
import glob
from typing import Dict, Literal, Optional, Tuple, Union
import json
from urllib.parse import ParseResult, urljoin, urlparse
import httpx
from olah.configs import OlahConfig
from olah.constants import WORKER_API_TIMEOUT

def get_org_repo(org: Optional[str], repo: str) -> str:
    if org is None:
        org_repo = repo
    else:
        org_repo = f"{org}/{repo}"
    return org_repo

def parse_org_repo(org_repo: str) -> Tuple[Optional[str], Optional[str]]:
    if "/" in org_repo and org_repo.count("/") != 1:
        return None, None
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo
    return org, repo

def get_meta_save_path(repos_path: str, repo_type: str, org: Optional[str], repo: str, commit: str) -> str:
    return os.path.join(repos_path, f"api/{repo_type}/{org}/{repo}/revision/{commit}/meta.json")

def get_meta_save_dir(repos_path: str, repo_type: str, org: Optional[str], repo: str) -> str:
    return os.path.join(repos_path, f"api/{repo_type}/{org}/{repo}/revision")

def get_file_save_path(repos_path: str, repo_type: str, org: Optional[str], repo: str, commit: str, file_path: str) -> str:
    return os.path.join(repos_path, f"heads/{repo_type}/{org}/{repo}/resolve_head/{commit}/{file_path}")

async def get_newest_commit_hf_offline(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: str, repo: str) -> str:
    repos_path = app.app_settings.repos_path
    save_dir = get_meta_save_dir(repos_path, repo_type, org, repo)
    files = glob.glob(os.path.join(save_dir, "*", "meta.json"))

    time_revisions = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            obj = json.loads(f.read())
            datetime_object = datetime.datetime.fromisoformat(obj["lastModified"])
            time_revisions.append((datetime_object, obj["sha"]))

    time_revisions = sorted(time_revisions)
    return time_revisions[-1][1]

async def get_newest_commit_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str) -> str:
    url = urljoin(app.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org}/{repo}")
    if app.app_settings.config.offline:
        return get_newest_commit_hf_offline(app, repo_type, org, repo)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=WORKER_API_TIMEOUT)
            if response.status_code != 200:
                return get_newest_commit_hf_offline(app, repo_type, org, repo)
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except:
        return get_newest_commit_hf_offline(app, repo_type, org, repo)

async def get_commit_hf_offline(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str, commit: str) -> str:
    repos_path = app.app_settings.repos_path
    save_path = get_meta_save_path(repos_path, repo_type, org, repo, commit)
    if os.path.exists(save_path):
        with open(save_path, "r", encoding="utf-8") as f:
            obj = json.loads(f.read())
        return obj["sha"]
    else:
        return None

async def get_commit_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str, commit: str) -> str:
    org_repo = get_org_repo(org, repo)
    url = urljoin(app.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}/revision/{commit}")
    if app.app_settings.config.offline:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=WORKER_API_TIMEOUT, follow_redirects=True)
            if response.status_code not in [200, 307]:
                return await get_commit_hf_offline(app, repo_type, org, repo, commit)
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)

async def check_commit_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str, commit: Optional[str]=None) -> bool:
    org_repo = get_org_repo(org, repo)
    if commit is None:
        url = urljoin(app.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}")
    else:
        url = urljoin(app.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}/revision/{commit}")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=WORKER_API_TIMEOUT)
    return response.status_code in [200, 307]

async def check_proxy_rules_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str) -> bool:
    config: OlahConfig = app.app_settings.config
    org_repo = get_org_repo(org, repo)
    return config.proxy.allow(org_repo)

async def check_cache_rules_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str) -> bool:
    config: OlahConfig = app.app_settings.config
    org_repo = get_org_repo(org, repo)
    return config.cache.allow(org_repo)

def get_url_tail(parsed_url: Union[str, ParseResult]) -> str:
    if isinstance(parsed_url, str):
        parsed_url = urlparse(parsed_url)
    url_tail = parsed_url.path
    if len(parsed_url.params) != 0:
        url_tail += f";{parsed_url.params}"
    if len(parsed_url.query) != 0:
        url_tail += f"?{parsed_url.query}"
    if len(parsed_url.fragment) != 0:
        url_tail += f"#{parsed_url.fragment}"
    return url_tail

def parse_range_params(file_range: str, file_size: int) -> Tuple[int, int]:
    # 'bytes=1887436800-'
    if file_range.startswith("bytes="):
        file_range = file_range[6:]
    start_pos, end_pos = file_range.split("-")
    if len(start_pos) != 0:
        start_pos = int(start_pos)
    else:
        start_pos = 0
    if len(end_pos) != 0:
        end_pos = int(end_pos)
    else:
        end_pos = file_size
    return start_pos, end_pos


class RemoteInfo(object):
    def __init__(self, method: str, url: str, headers: Dict[str, str]) -> None:
        self.method = method
        self.url = url
        self.headers = headers