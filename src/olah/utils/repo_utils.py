# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import datetime
import gzip
import os
import glob
import time
import tenacity
from typing import Dict, Literal, Optional, Tuple, Union
import json
from urllib.parse import urljoin
import httpx
from olah.constants import WORKER_API_TIMEOUT
from olah.utils.cache_utils import read_cache_request
from olah.utils.http_utils import create_api_client, is_transient_upstream_error


_CHECK_COMMIT_CACHE: Dict[Tuple[str, str, str, str], Tuple[bool, float]] = {}
_CHECK_COMMIT_INFLIGHT: Dict[Tuple[str, str, str, str], asyncio.Task] = {}
_GET_COMMIT_CACHE: Dict[Tuple[str, str, str, str], Tuple[str, float]] = {}
_GET_COMMIT_INFLIGHT: Dict[Tuple[str, str, str, str], asyncio.Task[Optional[str]]] = {}


def _commit_cache_key(
    repo_type: Optional[str],
    org_repo: str,
    commit: Optional[str],
    authorization: Optional[str],
) -> Tuple[str, str, str, str]:
    commit_key = commit if commit is not None else "__repo__"
    auth_key = "auth" if authorization else "noauth"
    return (str(repo_type), org_repo, commit_key, auth_key)


def _check_commit_cache_key(
    repo_type: Optional[str],
    org_repo: str,
    commit: Optional[str],
    authorization: Optional[str],
) -> Tuple[str, str, str, str]:
    return _commit_cache_key(repo_type, org_repo, commit, authorization)


def _get_commit_cache_ttl() -> float:
    raw = os.getenv("OLAH_GET_COMMIT_CACHE_TTL", "300")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 300.0


def _check_commit_cache_ttl(result: bool) -> float:
    if result:
        raw = os.getenv("OLAH_CHECK_COMMIT_CACHE_TTL", "120")
    else:
        raw = os.getenv("OLAH_CHECK_COMMIT_NEGATIVE_CACHE_TTL", "10")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 120.0 if result else 10.0


async def _check_commit_hf_upstream(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: Optional[str] = None,
    authorization: Optional[str] = None,
) -> bool:
    org_repo = get_org_repo(org, repo)
    if commit is None:
        url = urljoin(
            app.state.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}"
        )
    else:
        url = urljoin(
            app.state.app_settings.config.hf_url_base(),
            f"/api/{repo_type}/{org_repo}/revision/{commit}",
        )

    headers = {"Accept-Encoding": "identity"}
    if authorization is not None:
        headers["authorization"] = authorization
    try:
        async with create_api_client() as client:
            response = await client.request(
                method="HEAD",
                url=url,
                headers=headers,
            )
            status_code = response.status_code
    except httpx.HTTPError:
        return False
    return status_code in [200, 307]


@tenacity.retry(stop=tenacity.stop_after_attempt(3))
async def _check_commit_hf_with_retry(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: Optional[str] = None,
    authorization: Optional[str] = None,
) -> bool:
    return await _check_commit_hf_upstream(
        app, repo_type, org, repo, commit=commit, authorization=authorization
    )


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

    Concurrent callers for the same repo/commit share one upstream HEAD and
    cache the result briefly to avoid burst 401s during whole-repo downloads.
    """
    org_repo = get_org_repo(org, repo)
    key = _check_commit_cache_key(repo_type, org_repo, commit, authorization)
    now = time.monotonic()

    cached = _CHECK_COMMIT_CACHE.get(key)
    if cached is not None:
        result, expires_at = cached
        if now < expires_at:
            return result

    inflight = _CHECK_COMMIT_INFLIGHT.get(key)
    if inflight is not None:
        return await inflight

    async def _run() -> bool:
        try:
            result = await _check_commit_hf_with_retry(
                app,
                repo_type,
                org,
                repo,
                commit=commit,
                authorization=authorization,
            )
            if result:
                _CHECK_COMMIT_CACHE[key] = (
                    True,
                    time.monotonic() + _check_commit_cache_ttl(True),
                )
            return result
        finally:
            _CHECK_COMMIT_INFLIGHT.pop(key, None)

    task = asyncio.create_task(_run())
    _CHECK_COMMIT_INFLIGHT[key] = task
    return await task


def _load_cached_json_payload(request_cache: Dict[str, Union[bytes, Dict[str, str], int]]) -> Dict:
    content = request_cache["content"]
    headers = request_cache.get("headers", {})
    if isinstance(headers, dict) and headers.get("content-encoding") == "gzip":
        content = gzip.decompress(content)
    if isinstance(content, bytes):
        content = content.decode("utf-8")
    return json.loads(content)


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
    org_repo = get_org_repo(org, repo)
    return os.path.join(
        repos_path, f"api/{repo_type}/{org_repo}/revision/{commit}/meta_get.json"
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
    org_repo = get_org_repo(org, repo)
    return os.path.join(repos_path, f"api/{repo_type}/{org_repo}/revision")


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
    org_repo = get_org_repo(org, repo)
    return os.path.join(
        repos_path, f"heads/{repo_type}/{org_repo}/resolve_head/{commit}/{file_path}"
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
    repos_path = app.state.app_settings.config.repos_path
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
    org_repo = get_org_repo(org, repo)
    url = urljoin(
        app.state.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}"
    )
    if app.state.app_settings.config.offline:
        return await get_newest_commit_hf_offline(app, repo_type, org, repo)
    try:
        async with httpx.AsyncClient() as client:
            headers = {}
            if authorization is not None:
                headers["authorization"] = authorization
            response = await client.get(url, headers=headers, timeout=WORKER_API_TIMEOUT, follow_redirects=True)
            if response.status_code not in [200, 307]:
                return await get_newest_commit_hf_offline(app, repo_type, org, repo)
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except (httpx.HTTPError, ValueError, OSError):
        return await get_newest_commit_hf_offline(app, repo_type, org, repo)


async def _get_commit_hf_upstream(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: str,
    authorization: Optional[str] = None,
) -> Optional[str]:
    org_repo = get_org_repo(org, repo)
    url = urljoin(
        app.state.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/revision/{commit}",
    )
    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    async with create_api_client() as client:
        response = await client.get(url, headers=headers, follow_redirects=True)
        if response.status_code not in [200, 307]:
            return None
        obj = json.loads(response.text)
    return obj.get("sha", None)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception(is_transient_upstream_error),
    wait=tenacity.wait_exponential(multiplier=0.25, min=0.25, max=2.0),
    reraise=True,
)
async def _get_commit_hf_upstream_with_retry(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: str,
    authorization: Optional[str] = None,
) -> Optional[str]:
    return await _get_commit_hf_upstream(
        app, repo_type, org, repo, commit=commit, authorization=authorization
    )


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
    repos_path = app.state.app_settings.config.repos_path
    save_path = get_meta_save_path(repos_path, repo_type, org, repo, commit)
    if os.path.exists(save_path):
        request_cache = await read_cache_request(save_path)
        request_cache_json = _load_cached_json_payload(request_cache)
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

    Concurrent callers for the same repo/commit share one upstream GET and cache the
    resolved SHA briefly to avoid burst 401s during whole-repo downloads.
    """
    if app.state.app_settings.config.offline:
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)

    org_repo = get_org_repo(org, repo)
    key = _commit_cache_key(repo_type, org_repo, commit, authorization)
    now = time.monotonic()

    cached = _GET_COMMIT_CACHE.get(key)
    if cached is not None:
        sha, expires_at = cached
        if now < expires_at:
            return sha

    inflight = _GET_COMMIT_INFLIGHT.get(key)
    if inflight is not None:
        return await inflight

    async def _run() -> Optional[str]:
        try:
            try:
                sha = await _get_commit_hf_upstream_with_retry(
                    app,
                    repo_type,
                    org,
                    repo,
                    commit=commit,
                    authorization=authorization,
                )
            except (httpx.HTTPError, ValueError, OSError):
                sha = None
            if sha is None:
                sha = await get_commit_hf_offline(app, repo_type, org, repo, commit)
            if sha is not None:
                _GET_COMMIT_CACHE[key] = (
                    sha,
                    time.monotonic() + _get_commit_cache_ttl(),
                )
            return sha
        finally:
            _GET_COMMIT_INFLIGHT.pop(key, None)

    task = asyncio.create_task(_run())
    _GET_COMMIT_INFLIGHT[key] = task
    return await task

