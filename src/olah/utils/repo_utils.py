# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import datetime
import gzip
import logging
import os
import re
import glob
import tenacity
from typing import Dict, Literal, Optional, Tuple
import json
import zlib
from urllib.parse import urljoin
import httpx
from olah.constants import WORKER_API_TIMEOUT
from olah.errors import raise_if_rate_limited, UpstreamRateLimited
from olah.utils.cache_utils import cache_age, read_cache_request, read_cache_request_if_fresh, write_cache_request
from olah.utils.file_utils import make_dirs
from olah.utils.access_record import access_age, record_access
from olah.utils.auth_utils import token_hash
from olah.utils.upstream_fallback import is_offline

logger = logging.getLogger(__name__)

# Hub HEAD /api/{type}/{repo} is used as a visibility probe on every client
# request. huggingface_hub treats our 401 RepoNotFound as "model does not
# exist", so rate-limits and redirects must not be mapped onto that.
# Redirects are followed (follow_redirects on the AsyncClient below), so
# classification only ever sees the final status of the redirect chain.
_HF_VISIBILITY_RETRY = {408, 425}


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


# Metadata (repo visibility, branch -> SHA resolution) is requested per file and
# per worker during model loads. The per-key locks below collapse a concurrent
# stampede of identical lookups within one worker into a single upstream call;
# the fresh/stale disk cache additionally shares results across workers.
_metadata_locks: Dict[str, asyncio.Lock] = {}


def _metadata_lock(key: str) -> asyncio.Lock:
    """Per-key async lock guarding one metadata revalidation at a time.

    Locks are never removed: the key space is bounded by (repo, revision)
    pairs actually served, and a dict entry is ~100 bytes.
    """
    lock = _metadata_locks.get(key)
    if lock is None:
        lock = _metadata_locks.setdefault(key, asyncio.Lock())
    return lock


def _metadata_ttl(app) -> int:
    return getattr(app.state.app_settings.config, "metadata_cache_ttl", 0)


async def _metadata_cache_allowed(app, repo_type: Optional[str], org: Optional[str], repo: str) -> bool:
    """Cache rules gate metadata cache writes, exactly as for refs/meta/pathsinfo.

    Deferred import: rule_utils imports this module (get_org_repo), so a module
    level import would be circular.
    """
    from olah.utils.rule_utils import check_cache_rules_hf

    return await check_cache_rules_hf(app, repo_type, org, repo)


def _cacheable_headers(response: httpx.Response) -> Dict[str, str]:
    """Headers safe to persist next to a decoded response body.

    httpx returns ``response.content`` already decompressed, so wire-length and
    encoding headers no longer describe the stored bytes and must not be
    replayed to clients reading this cache (same normalization as refs).
    """
    headers = dict(response.headers)
    for name in ("content-encoding", "content-length", "transfer-encoding", "set-cookie"):
        headers.pop(name, None)
    return headers


def get_repo_get_save_path(repos_path: str, repo_type: str, org: Optional[str], repo: str) -> str:
    """Disk path of the TTL cache for the repo-root info GET (latest commit)."""
    org_repo = get_org_repo(org, repo)
    return os.path.join(repos_path, f"api/{repo_type}/{org_repo}/repo_get.json")


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

    Within the configured metadata TTL the cached repo info is reused without
    contacting Hugging Face; a stale copy is served when the upstream fails.

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
    if is_offline(app):
        return await get_newest_commit_hf_offline(app, repo_type, org, repo)

    ttl = _metadata_ttl(app)
    save_path = get_repo_get_save_path(
        app.state.app_settings.config.repos_path, repo_type, org, repo
    )
    if ttl > 0:
        cached = await read_cache_request_if_fresh(save_path, ttl)
        if cached is not None:
            try:
                return _load_cached_json_payload(cached).get("sha")
            except (ValueError, UnicodeDecodeError, zlib.error):
                pass
    allow_cache = ttl > 0 and await _metadata_cache_allowed(app, repo_type, org, repo)
    try:
        headers = {}
        if authorization is not None:
            headers["authorization"] = authorization
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=WORKER_API_TIMEOUT, follow_redirects=True)
            if response.status_code not in [200, 307]:
                cached = await get_newest_commit_hf_offline(app, repo_type, org, repo)
                if cached is None:
                    raise_if_rate_limited(response)
                return cached
            obj = json.loads(response.text)
        if allow_cache:
            make_dirs(save_path)
            await write_cache_request(
                save_path, 200, _cacheable_headers(response), response.content
            )
        return obj.get("sha", None)
    except (httpx.HTTPError, ValueError, OSError):
        return await get_newest_commit_hf_offline(app, repo_type, org, repo)


def is_full_commit_hash(commit: str) -> bool:
    """Only full Git SHA-1 IDs are immutable; short IDs still need resolution."""
    return re.fullmatch(r"[0-9a-fA-F]{40}", commit) is not None


def get_resolved_commit_save_path(
    repos_path: str, repo_type: str, org: Optional[str], repo: str, revision: str
) -> str:
    """Disk path of the revision -> commit mapping recorded by the file-route probe."""
    revision_dir = os.path.dirname(get_meta_save_path(repos_path, repo_type, org, repo, revision))
    return os.path.join(revision_dir, "resolved_commit.json")


async def record_resolved_commit(
    app, repo_type: str, org: Optional[str], repo: str, revision: str, commit: str
) -> None:
    if is_full_commit_hash(revision) or not await _metadata_cache_allowed(app, repo_type, org, repo):
        return
    save_path = get_resolved_commit_save_path(
        app.state.app_settings.config.repos_path, repo_type, org, repo, revision
    )
    make_dirs(save_path)
    await write_cache_request(save_path, 200, {}, commit.encode("ascii"))


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
    # A full SHA is already a cache address, independent of whether revision
    # metadata was cached. Access and visibility remain the caller's checks.
    if is_full_commit_hash(commit):
        return commit.lower()
    repos_path = app.state.app_settings.config.repos_path
    meta_path = get_meta_save_path(repos_path, repo_type, org, repo, commit)
    resolved_path = get_resolved_commit_save_path(repos_path, repo_type, org, repo, commit)
    # Both the metadata routes and file downloads record where a branch points;
    # the more recent one wins.
    def age(path: str) -> float:
        seconds = cache_age(path)
        return float("inf") if seconds is None else seconds

    candidates = sorted((p for p in (meta_path, resolved_path) if os.path.exists(p)), key=age)
    for path in candidates:
        try:
            request_cache = await read_cache_request(path)
            if path == resolved_path:
                sha = request_cache["content"].decode("ascii").strip()
            else:
                sha = _load_cached_json_payload(request_cache).get("sha")
        except (ValueError, OSError, KeyError, UnicodeDecodeError, zlib.error):
            # Corrupt or unreadable cache: treat as a cache miss so the caller can
            # surface "commit not found" instead of crashing with a 500.
            continue
        if sha:
            return sha
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
    if is_full_commit_hash(commit):
        return commit.lower()
    org_repo = get_org_repo(org, repo)
    url = urljoin(
        app.state.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/revision/{commit}",
    )
    if is_offline(app):
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
                cached = await get_commit_hf_offline(app, repo_type, org, repo, commit)
                if cached is None:
                    raise_if_rate_limited(response)
                return cached
            obj = json.loads(response.text)
        return obj.get("sha", None)
    except (httpx.HTTPError, ValueError, OSError):
        return await get_commit_hf_offline(app, repo_type, org, repo, commit)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=0.5, max=2),
    retry=tenacity.retry_if_result(lambda result: result[0] is None),
    retry_error_callback=lambda retry_state: (None, None),
)
async def _probe_hf(
    url: str, method: str, authorization: Optional[str]
) -> Tuple[Optional[bool], Optional[httpx.Response]]:
    """Request a Hub API URL and classify the final status (see check_commit_hf).

    Raises:
        UpstreamRateLimited: on a 429, without retrying: retrying immediately
        only extends the rate limit, and the client can wait for the reset
        window given in the relayed headers.
    """
    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
            )
    except httpx.HTTPError as e:
        logger.warning("Upstream request failed while checking %s: %r", url, e)
        return None, None
    raise_if_rate_limited(response)
    status_code = response.status_code
    if status_code in _HF_VISIBILITY_RETRY or status_code >= 500:
        return None, response
    return 200 <= status_code < 300, response


async def record_caller_access(
    app, repo_type: str, org: Optional[str], repo: str, authorization: Optional[str]
) -> None:
    """Record that the Hub just confirmed this caller's access, for the TTLs that reuse it."""
    config = app.state.app_settings.config
    if _metadata_ttl(app) <= 0 and getattr(config, "metadata_stale_if_error", 0) <= 0:
        return
    if not await _metadata_cache_allowed(app, repo_type, org, repo):
        return
    record_access(config.repos_path, repo_type, org, repo, token_hash(authorization))


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
        True if the commit is valid (a final 2xx status once redirects have
        been followed), False if the upstream rejected it (other
        non-retryable status), or None if the upstream could not be reached
        (transport error, 408/425, or 5xx).
        None is retried by the decorator; callers map it to HTTP 504 rather
        than 401 so huggingface_hub does not treat a blip as "repo not found".

    Raises:
        UpstreamRateLimited: on a 429, without retrying: retrying immediately
        only extends the rate limit, and the client can wait for the reset
        window given in the relayed headers.

    """
    org_repo = get_org_repo(org, repo)
    ttl = _metadata_ttl(app)
    repos_path = app.state.app_settings.config.repos_path
    if commit is None:
        # Visibility depends on the caller's token, so it is cached per caller.
        url = urljoin(
            app.state.app_settings.config.hf_url_base(), f"/api/{repo_type}/{org_repo}"
        )
        caller = token_hash(authorization)
        age = access_age(repos_path, repo_type, org, repo, caller)
        if ttl > 0 and age is not None and age < ttl:
            return True
        exists, _ = await _probe_hf(url, "HEAD", authorization)
        if exists is True:
            await record_caller_access(app, repo_type, org, repo, authorization)
        return exists

    url = urljoin(
        app.state.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/revision/{commit}",
    )
    # Same envelope the client-facing HEAD route persists under
    # revision/{commit}/meta_head.json, so both writers feed one cache entry
    # per revision. Shared across callers, who have each passed the repo check.
    revision_dir = os.path.dirname(
        get_meta_save_path(repos_path, repo_type, org, repo, commit)
    )
    save_path = os.path.join(revision_dir, "meta_head.json")
    if ttl > 0 and await read_cache_request_if_fresh(save_path, ttl) is not None:
        return True
    exists, _ = await _probe_hf(url, "HEAD", authorization)
    if (
        exists is True
        and ttl > 0
        and await _metadata_cache_allowed(app, repo_type, org, repo)
    ):
        make_dirs(save_path)
        await write_cache_request(save_path, 200, {}, b"")
    return exists


async def lookup_commit_hf(
    app,
    repo_type: Optional[Literal["models", "datasets", "spaces"]],
    org: Optional[str],
    repo: str,
    commit: str,
    authorization: Optional[str] = None,
) -> Tuple[Optional[bool], Optional[str]]:
    """Check a revision and resolve it to a commit SHA in one upstream request.

    Within the configured metadata TTL the branch/tag -> SHA mapping is served
    from the revision metadata cache (the same ``revision/{rev}/meta_get.json``
    envelope the client-facing metadata route and offline mode use), so file
    downloads that address a branch stop re-resolving it against the Hub on
    every request. Concurrent lookups of the same revision inside one worker
    share a single upstream call; when revalidation fails but a stale entry
    exists, the stale SHA is served instead of erroring, which keeps clients
    retrying against the cache rather than burning rate-limit quota.

    Returns ``(exists, sha)``: ``exists`` is classified exactly as
    ``check_commit_hf`` does, and ``sha`` is set only when the revision exists
    and the Hub returned a usable payload. Only positive results are cached.
    """
    ttl = _metadata_ttl(app)
    save_path = get_meta_save_path(
        app.state.app_settings.config.repos_path, repo_type, org, repo, commit
    )

    def _sha_from(cached: Dict) -> Optional[str]:
        try:
            return _load_cached_json_payload(cached).get("sha")
        except (ValueError, UnicodeDecodeError, zlib.error):
            return None

    if ttl > 0:
        cached = await read_cache_request_if_fresh(save_path, ttl)
        if cached is not None:
            cached_sha = _sha_from(cached)
            if cached_sha is not None:
                return True, cached_sha

    org_repo = get_org_repo(org, repo)
    url = urljoin(
        app.state.app_settings.config.hf_url_base(),
        f"/api/{repo_type}/{org_repo}/revision/{commit}",
    )
    lock = _metadata_lock(f"revision:{repo_type}:{org_repo}:{commit}")
    async with lock:
        allow_cache = ttl > 0 and await _metadata_cache_allowed(app, repo_type, org, repo)
        # Another coroutine may have revalidated while this caller waited.
        if ttl > 0:
            cached = await read_cache_request_if_fresh(save_path, ttl)
            if cached is not None:
                cached_sha = _sha_from(cached)
                if cached_sha is not None:
                    return True, cached_sha
        try:
            exists, response = await _probe_hf(url, "GET", authorization)
        except (UpstreamRateLimited, httpx.HTTPError):
            # A rate-limited or unreachable upstream must not turn a cached
            # revision into an error: serving the last known SHA keeps the
            # mirror usable and stops the client retry loop from spending
            # whatever quota remains.
            if ttl > 0 and os.path.exists(save_path):
                stale = await _read_stale_sha(save_path)
                if stale is not None:
                    logger.warning(
                        "Upstream failed for %s; serving stale revision metadata", url
                    )
                    return True, stale
            raise
        if exists is None:
            # Upstream unreachable (transport error, 408/425, 5xx after
            # retries): same stale-serving rationale as the exception path.
            if ttl > 0 and os.path.exists(save_path):
                stale = await _read_stale_sha(save_path)
                if stale is not None:
                    logger.warning(
                        "Upstream unavailable for %s; serving stale revision metadata", url
                    )
                    return True, stale
            return None, None
        if not exists:
            return exists, None
        try:
            sha = response.json().get("sha")
        except (ValueError, AttributeError):
            sha = None
        if allow_cache and sha is not None:
            make_dirs(save_path)
            await write_cache_request(
                save_path, 200, _cacheable_headers(response), response.content
            )
        return True, sha


async def _read_stale_sha(save_path: str) -> Optional[str]:
    """Best-effort SHA from a stale (age-expired) revision cache entry."""
    try:
        cached = await read_cache_request(save_path)
        if cached.get("status_code") != 200:
            return None
        return _load_cached_json_payload(cached).get("sha")
    except (OSError, ValueError, KeyError, zlib.error):
        return None
