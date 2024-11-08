# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import datetime
import os
import glob
import tenacity
from typing import Dict, Literal, Optional, Tuple, Union
import json
from urllib.parse import urljoin
import httpx
from olah.constants import WORKER_API_TIMEOUT
from olah.utils.cache_utils import read_cache_request


def get_org_repo(org: Optional[str], repo: str) -> str:
    """
    Constructs the organization/repository name.

    Args:
        org: The organization name (optional).
        repo: The repository name.

    Returns:
        The organization/repository name as a string.

    """
    if org is None:
        org_repo = repo
    else:
        org_repo = f"{org}/{repo}"
    return org_repo


def parse_org_repo(org_repo: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses the organization/repository name.

    Args:
        org_repo: The organization/repository name.

    Returns:
        A tuple containing the organization name and repository name.

    """
    if "/" in org_repo and org_repo.count("/") != 1:
        return None, None
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo
    return org, repo


def get_meta_save_path(
    repos_path: str, repo_type: str, org: Optional[str], repo: str, commit: str
) -> str:
    """
    Constructs the path to save the meta.json file.

    Args:
        repos_path: The base path where repositories are stored.
        repo_type: The type of repository.
        org: The organization name (optional).
        repo: The repository name.
        commit: The commit hash.

    Returns:
        The path to save the meta.json file as a string.

    """
    return os.path.join(
        repos_path, f"api/{repo_type}/{org}/{repo}/revision/{commit}/meta_get.json"
    )


def get_meta_save_dir(
    repos_path: str, repo_type: str, org: Optional[str], repo: str
) -> str:
    """
    Constructs the directory path to save the meta.json file.

    Args:
        repos_path: The base path where repositories are stored.
        repo_type: The type of repository.
        org: The organization name (optional).
        repo: The repository name.

    Returns:
        The directory path to save the meta.json file as a string.

    """
    return os.path.join(repos_path, f"api/{repo_type}/{org}/{repo}/revision")


def get_file_save_path(
    repos_path: str,
    repo_type: str,
    org: Optional[str],
    repo: str,
    commit: str,
    file_path: str,
) -> str:
    """
    Constructs the path to save a file in the repository.

    Args:
        repos_path: The base path where repositories are stored.
        repo_type: The type of repository.
        org: The organization name (optional).
        repo: The repository name.
        commit: The commit hash.
        file_path: The path of the file within the repository.

    Returns:
        The path to save the file as a string.

    """
    return os.path.join(
        repos_path, f"heads/{repo_type}/{org}/{repo}/resolve_head/{commit}/{file_path}"
    )


async def get_newest_commit_hf_offline(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: str,
    repo: str,
) -> Optional[str]:
    """
    Retrieves the newest commit hash for a repository in offline mode.

    Args:
        app: The application object.
        repo_type: The type of repository.
        org: The organization name.
        repo: The repository name.

    Returns:
        The newest commit hash as a string.

    """
    repos_path = app.app_settings.config.repos_path
    save_dir = get_meta_save_dir(repos_path, repo_type, org, repo)
    files = glob.glob(os.path.join(save_dir, "*", "meta_head.json"))

    time_revisions = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            obj = json.loads(f.read())
            datetime_object = datetime.datetime.fromisoformat(obj["lastModified"])
            time_revisions.append((datetime_object, obj["sha"]))

    time_revisions = sorted(time_revisions)
    if len(time_revisions) == 0:
        return None
    else:
        return time_revisions[-1][1]


async def get_newest_commit_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    authorization: Optional[str] = None,
) -> Optional[str]:
    """
    Retrieves the newest commit hash for a repository.

    Args:
        app: The application object.
        repo_type: The type of repository.
        org: The organization name (optional).
        repo: The repository name.

    Returns:
        The newest commit hash as a string, or None if it cannot be obtained.

    """
    url = urljoin(
        app.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org}/{repo}"
    )
    if app.app_settings.config.offline:
        return await get_newest_commit_hf_offline(app, repo_type, org, repo)
    try:
        async with httpx.AsyncClient() as client:
            headers = {}
            if authorization is not None:
                headers["authorization"] = authorization
            response = await client.get(url, headers=headers, timeout=WORKER_API_TIMEOUT)
            if response.status_code != 200:
                return await get_newest_commit_hf_offline(app, repo_type, org, repo)
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except httpx.TimeoutException as e:
        return await get_newest_commit_hf_offline(app, repo_type, org, repo)


async def get_commit_hf_offline(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: str,
) -> Optional[str]:
    """
    Retrieves the commit SHA for a given repository and commit from the offline cache.

    This function is used when the application is in offline mode and the commit information is not available from the API.

    Args:
        app: The application instance.
        repo_type: Optional. The type of repository ("models", "datasets", or "spaces").
        org: Optional. The organization name for the repository.
        repo: The name of the repository.
        commit: The commit identifier.

    Returns:
        The commit SHA as a string if available in the offline cache, or None if the information is not cached.
    """
    repos_path = app.app_settings.config.repos_path
    save_path = get_meta_save_path(repos_path, repo_type, org, repo, commit)
    if os.path.exists(save_path):
        request_cache = await read_cache_request(save_path)
        request_cache_json = json.loads(request_cache["content"])
        return request_cache_json["sha"]
    else:
        return None


async def get_commit_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: str,
    authorization: Optional[str] = None,
) -> Optional[str]:
    """
    Retrieves the commit SHA for a given repository and commit from the Hugging Face API.

    Args:
        app: The application instance.
        repo_type: Optional. The type of repository ("models", "datasets", or "spaces").
        org: Optional. The organization name for the repository.
        repo: The name of the repository.
        commit: The commit identifier.
        authorization: Optional. The authorization token for accessing the API.

    Returns:
        The commit SHA as a string, or None if the commit cannot be retrieved.

    Raises:
        This function does not raise any explicit exceptions but may propagate exceptions from underlying functions.
    """
    org_repo = get_org_repo(org, repo)
    url = urljoin(
        app.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/revision/{commit}",
    )
    if app.app_settings.config.offline:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)
    try:
        headers = {}
        if authorization is not None:
            headers["authorization"] = authorization
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url, headers=headers, timeout=WORKER_API_TIMEOUT, follow_redirects=True
            )
            if response.status_code not in [200, 307]:
                return await get_commit_hf_offline(app, repo_type, org, repo, commit)
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)


@tenacity.retry(stop=tenacity.stop_after_attempt(3))
async def check_commit_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: Optional[str] = None,
    authorization: Optional[str] = None,
) -> bool:
    """
    Checks the commit status of a repository in the Hugging Face ecosystem.

    Args:
        app: The application object.
        repo_type: The type of repository (models, datasets, or spaces).
        org: The organization name (optional).
        repo: The repository name.
        commit: The commit hash (optional).
        authorization: The authorization token (optional).

    Returns:
        A boolean indicating if the commit is valid (status code 200 or 307) or not.

    """
    org_repo = get_org_repo(org, repo)
    if commit is None:
        url = urljoin(
            app.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}"
        )
    else:
        url = urljoin(
            app.app_settings.config.hf_url_base(),
            f"/api/{repo_type}/{org_repo}/revision/{commit}",
        )

    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    async with httpx.AsyncClient() as client:
        response = await client.request(method="HEAD", url=url, headers=headers, timeout=WORKER_API_TIMEOUT)
        status_code = response.status_code
    return status_code in [200, 307]
