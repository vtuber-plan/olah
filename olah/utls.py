
import datetime
import os
import glob
from typing import Literal, Optional
import json
import httpx
from olah.configs import OlahConfig
from olah.constants import WORKER_API_TIMEOUT

def get_org_repo(org: Optional[str], repo: str) -> str:
    if org is None:
        org_repo = repo
    else:
        org_repo = f"{org}/{repo}"
    return org_repo

def get_meta_save_path(repos_path: str, repo_type: str, org: Optional[str], repo: str, commit: str) -> str:
    return os.path.join(repos_path, f"api/{repo_type}/{org}/{repo}/revision/{commit}")

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
    url = f"{app.app_settings.hf_url}/api/{repo_type}/{org}/{repo}"
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

    with open(save_path, "r", encoding="utf-8") as f:
        obj = json.loads(f.read())

    return obj["sha"]

async def get_commit_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str, commit: str) -> str:
    org_repo = get_org_repo(org, repo)
    url = f"{app.app_settings.hf_url}/api/{repo_type}/{org_repo}/revision/{commit}"
    if app.app_settings.config.offline:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url,
                    timeout=WORKER_API_TIMEOUT)
            if response.status_code != 200:
                return await get_commit_hf_offline(app, repo_type, org, repo, commit)
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)

async def check_commit_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str, commit: Optional[str]=None) -> bool:
    org_repo = get_org_repo(org, repo)
    if commit is None:
        url = f"{app.app_settings.hf_url}/api/{repo_type}/{org_repo}"
    else:
        url = f"{app.app_settings.hf_url}/api/{repo_type}/{org_repo}/revision/{commit}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=WORKER_API_TIMEOUT)
    return response.status_code in [200, 307]

async def check_proxy_rules_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str) -> bool:
    config: OlahConfig = app.app_settings.config
    org_repo = get_org_repo(org, repo)
    return config.proxy.allow(f"{org_repo}")

async def check_cache_rules_hf(app, repo_type: Optional[Literal["models", "datasets", "spaces"]], org: Optional[str], repo: str) -> bool:
    config: OlahConfig = app.app_settings.config
    org_repo = get_org_repo(org, repo)
    return config.cache.allow(f"{org_repo}")
