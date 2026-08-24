# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import datetime
import gzip
import logging
import os
import glob
import tenacity
from typing import Dict, Literal, Optional, Tuple
import json
import zlib
from urllib.parse import urljoin
import httpx
from olah.constants import WORKER_API_TIMEOUT
from olah.utils.cache_utils import read_cache_request

logger = logging.getLogger(__name__)

# Hub HEAD /api/{type}/{repo} is used as a visibility probe on every client
# request. huggingface_hub treats our 401 RepoNotFound as "model does not
# exist", so rate-limits and redirects must not be mapped onto that.
_HF_VISIBILITY_OK = {200, 301, 302, 303, 307, 308}
_HF_VISIBILITY_RETRY = {408, 425, 429}


def _content_encoding_is_gzip(headers: object) -> bool:
    """Return True if the cached response headers advertise gzip content-encoding.

    Header names are matched case-insensitively and the value is split on commas
    so that compound encodings such as ``"gzip, br"`` and variants like
    ``"x-gzip"`` are recognised.
    """
    if not isinstance(headers, dict):
        return False
    for key, value in headers.items():
        if str(key).lower() == "content-encoding":
            tokens = [token.strip().lower() for token in str(value).split(",")]
            return any(token in ("gzip", "x-gzip") for token in tokens)
    return False


def _load_cached_json_payload(request_cache: Dict) -> Dict:
    """Decode and JSON-parse the body of a cached API response.

    Cached bodies are stored verbatim as captured upstream via ``aiter_raw()``;
    when the upstream response was gzip-compressed the raw bytes are still gzip.
    Decompression is triggered when either the cached ``content-encoding`` header
    advertises gzip *or* the body begins with the gzip magic bytes
    (``\\x1f\\x8b``). The magic-byte fallback self-heals older caches whose
    headers were not recorded as gzip.
    """
    content = request_cache["content"]
    headers = request_cache.get("headers", {})

    looks_like_gzip = _content_encoding_is_gzip(headers) or (
        isinstance(content, (bytes, bytearray))
        and bytes(content[:2]) == b"\x1f\x8b"
    )
    if looks_like_gzip:
        try:
            content = gzip.decompress(content)
        except (OSError, EOFError, zlib.error):
            # Truncated or corrupt gzip stream: fall back to the raw bytes and
            # let the decode / json step below surface a clearer error.
            pass

    if isinstance(content, (bytes, bytearray)):
        content = bytes(content).decode("utf-8")
    return json.loads(content)


def _load_meta_head_object(file_path: str) -> Optional[Dict]:
    """Load a ``meta_head.json`` revision metadata file.

    The file may be stored in either of two schemas:

    * a plain mirror ``RepoMeta`` document (``{"sha": ..., "lastModified": ...}``)
    * a proxy HTTP cache envelope written by ``write_cache_request``
      (``{"status_code", "headers"}`` with the body in a sibling ``.body``
      sidecar; legacy envelopes also accepted with the body inline as hex
      ``content``). HEAD caches carry an empty body and therefore contribute no
      revision info.

    Returns the inner revision object, or ``None`` if the file cannot be parsed
    as revision metadata.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.loads(f.read())
    except (OSError, ValueError):
        return None

    body_path = file_path + ".body"
    is_envelope = isinstance(raw, dict) and (
        "status_code" in raw or os.path.exists(body_path)
    )
    if is_envelope:
        # Proxy cache envelope written by write_cache_request: the body lives in
        # a sibling .body sidecar (new) or inline as hex (legacy).
        try:
            if os.path.exists(body_path):
                with open(body_path, "rb") as bf:
                    content = bf.read()
            else:
                content = bytes.fromhex(raw.get("content", ""))
            request_cache = {"content": content, "headers": raw.get("headers", {})}
            return _load_cached_json_payload(request_cache)
        except (ValueError, OSError, EOFError, zlib.error):
            return None
    return raw if isinstance(raw, dict) else None


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
        meta = _load_meta_head_object(file)
        if not isinstance(meta, dict):
            continue
        sha = meta.get("sha")
        last_modified = meta.get("lastModified")
        if not sha or not last_modified:
            continue
        try:
            datetime_object = datetime.datetime.fromisoformat(last_modified)
        except ValueError:
            continue
        time_revisions.append((datetime_object, sha))

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
    if not os.path.exists(save_path):
        return None
    try:
        request_cache = await read_cache_request(save_path)
        request_cache_json = _load_cached_json_payload(request_cache)
    except (ValueError, OSError, zlib.error):
        # Corrupt or unreadable cache: treat as a cache miss so the caller can
        # surface "commit not found" instead of crashing with a 500.
        return None
    return request_cache_json.get("sha")


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
        app.state.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/revision/{commit}",
    )
    if app.state.app_settings.config.offline:
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
    except (httpx.HTTPError, ValueError, OSError):
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=0.5, max=2),
    retry=tenacity.retry_if_result(lambda result: result is None),
    retry_error_callback=lambda retry_state: None,
)
async def check_commit_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: Optional[str] = None,
    authorization: Optional[str] = None,
) -> Optional[bool]:
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
        True if the commit is valid (2xx or redirect), False if the
        upstream rejected it (other non-retryable status), or None if the
        upstream could not be reached (transport error, 429, or 5xx).
        None is retried by the decorator; callers map it to HTTP 504 rather
        than 401 so huggingface_hub does not treat a blip as "repo not found".

    """
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

    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.request(
                method="HEAD",
                url=url,
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
            )
            status_code = response.status_code
    except httpx.HTTPError as e:
        logger.warning("Upstream request failed while checking %s: %r", url, e)
        return None
    if status_code in _HF_VISIBILITY_RETRY or status_code >= 500:
        return None
    return status_code in _HF_VISIBILITY_OK
