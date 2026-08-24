# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Content-identity registry + authorization for LFS objects.

LFS download URLs (``/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}``) are
content-addressed: ``hash_file`` is the SHA-256 of the blob, but the URL carries
no repo identity, so a cache hit cannot be re-authorized against repo visibility
on its own. We therefore keep a small registry mapping ``content_hash -> {repos}``
populated when olah proxies an authenticated ``resolve`` (where it learns both the
repo and the file's LFS oid from the paths-info response).

Authorization is **fail-closed**: an object with no registered repos is never
served (404), and an object whose candidate repos all fail a visibility probe is
denied (403). Because the object is content-addressed, a requester is entitled to
the bytes if they can access *any* repo that contains them.

Offline mode skips authorization entirely (consistent with olah's "trust the local
cache" offline semantics): a cached blob is served, an uncached one cannot be
fetched.
"""

import hashlib
import json
import os
import threading
import time
from typing import List, Optional, Tuple

import portalocker

# In-process memo for (token, repo) -> authorized, to avoid re-probing HF on
# every cache hit. Short TTL so visibility changes propagate.
_VIS_TTL = 60.0
_vis_cache: dict = {}  # (token_hash, repo_key) -> (ts, authorized)
_vis_lock = threading.Lock()


def _index_path(repos_path: str, content_hash: str) -> str:
    return os.path.join(
        repos_path, "lfs", "object_index", content_hash[:2], content_hash + ".json"
    )


def _token_hash(authorization: Optional[str]) -> str:
    if not authorization:
        return "anon"
    return hashlib.sha256(authorization.encode("utf-8")).hexdigest()[:16]


async def register_lfs_object(
    app, repo_type: str, org: Optional[str], repo: str, content_hash: str
) -> None:
    """Record that ``content_hash`` is reachable via ``(repo_type, org, repo)``.

    Best-effort: a registration failure must never break a request. Idempotent.
    """
    if not content_hash:
        return
    try:
        repos_path = app.state.app_settings.config.repos_path
        path = _index_path(repos_path, content_hash)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        lock_path = path + ".lock"
        # Cross-process read-modify-write guard; the lock file content is unused.
        with portalocker.Lock(lock_path, "w", timeout=10, flags=portalocker.LOCK_EX):
            entry = {"content_hash": content_hash, "repos": []}
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        loaded = json.load(f)
                    if isinstance(loaded, dict):
                        entry = loaded
                except (json.JSONDecodeError, OSError):
                    entry = {"content_hash": content_hash, "repos": []}
            repos = entry.setdefault("repos", [])
            key = [repo_type, org, repo]
            if key not in repos:
                repos.append(key)
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(entry, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
    except Exception:
        pass


async def get_lfs_object_repos(
    app, content_hash: str
) -> List[Tuple[str, Optional[str], str]]:
    """Return the candidate repos registered for ``content_hash``."""
    if not content_hash:
        return []
    try:
        repos_path = app.state.app_settings.config.repos_path
        path = _index_path(repos_path, content_hash)
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            entry = json.load(f)
        out: List[Tuple[str, Optional[str], str]] = []
        for r in entry.get("repos", []):
            if isinstance(r, list) and len(r) == 3:
                out.append((r[0], r[1], r[2]))
        return out
    except Exception:
        return []


async def _visibility_cached(
    app, repo_type: str, org: Optional[str], repo: str, authorization: Optional[str]
) -> bool:
    """Memoized repo-visibility probe (re-uses server_access.ensure_repo_visibility).

    Only definitive outcomes are cached. A transient upstream failure (504
    ProxyTimeout, e.g. a rate-limited or unreachable Hub) must not be cached
    as "not visible": that would fail-close LFS/Xet authorization for the
    whole TTL instead of retrying on the next request.
    """
    from olah.server_access import build_repo_ref, ensure_repo_visibility

    repo_key = f"{repo_type}/{org}/{repo}"
    th = _token_hash(authorization)
    now = time.time()
    with _vis_lock:
        rec = _vis_cache.get((th, repo_key))
        if rec and now - rec[0] < _VIS_TTL:
            return rec[1]
    ref = build_repo_ref(repo_type, org, repo)
    err = await ensure_repo_visibility(app, ref, authorization)
    ok = err is None
    transient = err is not None and err.status_code == 504
    with _vis_lock:
        if not transient:
            _vis_cache[(th, repo_key)] = (now, ok)
    return ok


async def authorize_lfs_object(
    app, content_hash: str, authorization: Optional[str]
) -> Optional[int]:
    """Authorize an LFS request for ``content_hash``.

    Returns ``None`` to proceed, or ``403`` to deny. Semantics:

    * **Known + accessible** (a registered candidate repo the requester can see,
      or offline mode): proceed -- caching is then governed by the cache rules.
    * **Known + denied** (candidates exist but the requester can access none):
      deny with 403. This is the fail-closed gate that makes caching safe.
    * **Unknown** (no candidate repos registered): proceed by deferring to
      upstream, where HF enforces access. Such objects are *never* cached (see
      ``cache_allowed_for_lfs_object``), because their authorization could not be
      re-validated on a later cache hit. This preserves transparent-proxy use.
    """
    if app.state.app_settings.config.offline:
        return None
    candidates = await get_lfs_object_repos(app, content_hash)
    if not candidates:
        return None  # unknown -> defer to upstream (HF enforces), do not cache
    for (repo_type, org, repo) in candidates:
        if await _visibility_cached(app, repo_type, org, repo, authorization):
            return None
    return 403


async def cache_allowed_for_lfs_object(app, content_hash: str) -> bool:
    """Whether caching is permitted for this object per the cache allow-rules.

    Resolved against the registered candidate repos: caching is allowed if *any*
    candidate repo matches the operator's cache rules. Unknown objects (no
    candidates) are never cached, since their authorization cannot be
    re-validated on a cache hit.
    """
    from olah.utils.rule_utils import check_cache_rules_hf

    candidates = await get_lfs_object_repos(app, content_hash)
    for (repo_type, org, repo) in candidates:
        if await check_cache_rules_hf(app, repo_type, org, repo):
            return True
    return False


# ---------------------------------------------------------------------------
# Xet objects
# ---------------------------------------------------------------------------
# Xet direct links (/xet-bridge-{region}/{repo_hash}/{xet_hash}) carry only the
# content hash, never the repo/file. To re-resolve a fresh signed download URL
# on a cache miss (the signed URL expires) we must know (repo, file, commit), so
# the xet index stores full refs alongside the size and the matching lfs oid.

def _xet_index_path(repos_path: str, xet_hash: str) -> str:
    return os.path.join(
        repos_path, "lfs", "xet_index", xet_hash[:2], xet_hash + ".json"
    )


async def register_xet_object(
    app,
    repo_type: str,
    org: Optional[str],
    repo: str,
    file_path: str,
    commit: Optional[str],
    lfs_oid: str,
    xet_hash: str,
    size: int,
) -> None:
    """Record a Xet object: xet_hash -> (repo, file, commit) + size + lfs_oid.

    Best-effort and idempotent. Populated at resolve time so the Xet content
    route can authorize and re-resolve later.
    """
    if not xet_hash:
        return
    try:
        repos_path = app.state.app_settings.config.repos_path
        path = _xet_index_path(repos_path, xet_hash)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with portalocker.Lock(path + ".lock", "w", timeout=10, flags=portalocker.LOCK_EX):
            entry = {"xet_hash": xet_hash, "lfs_oid": lfs_oid, "size": size, "refs": []}
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        loaded = json.load(f)
                    if isinstance(loaded, dict):
                        entry = loaded
                except (json.JSONDecodeError, OSError):
                    entry = {"xet_hash": xet_hash, "lfs_oid": lfs_oid, "size": size, "refs": []}
            entry.setdefault("lfs_oid", lfs_oid)
            entry.setdefault("size", size)
            refs = entry.setdefault("refs", [])
            ref = [repo_type, org, repo, file_path, commit]
            if ref not in refs:
                refs.append(ref)
                tmp = path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(entry, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
    except Exception:
        pass


async def get_xet_metadata(app, xet_hash: str):
    """Return ``(refs, size, lfs_oid)`` for ``xet_hash``.

    ``refs`` is a list of ``(repo_type, org, repo, file_path, commit)`` tuples
    (or ``[]`` if unknown). Used by the Xet route for authorization + re-resolve.
    """
    if not xet_hash:
        return [], None, None
    try:
        repos_path = app.state.app_settings.config.repos_path
        path = _xet_index_path(repos_path, xet_hash)
        if not os.path.exists(path):
            return [], None, None
        with open(path, "r", encoding="utf-8") as f:
            entry = json.load(f)
        refs = []
        for r in entry.get("refs", []):
            if isinstance(r, list) and len(r) >= 3:
                # (repo_type, org, repo, [file_path], [commit])
                refs.append(tuple(r[:5]) if len(r) >= 5 else (r[0], r[1], r[2], None, None))
        return refs, entry.get("size"), entry.get("lfs_oid")
    except Exception:
        return [], None, None


async def authorize_xet_object(
    app, xet_hash: str, authorization: Optional[str]
) -> Optional[int]:
    """Authorize a Xet request. Returns ``None`` to proceed or ``403`` to deny.

    Offline trusts the local cache. A registered object whose candidate repos
    all fail visibility is denied (fail-closed). Callers should 404 first if
    ``get_xet_metadata`` returns no refs (unknown objects cannot be re-resolved).
    """
    if app.state.app_settings.config.offline:
        return None
    refs, _size, _oid = await get_xet_metadata(app, xet_hash)
    if not refs:
        # Unknown -> the route returns 404 (cannot re-resolve without a ref).
        return None
    for (repo_type, org, repo, _file, _commit) in refs:
        if await _visibility_cached(app, repo_type, org, repo, authorization):
            return None
    return 403

