# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
import shutil
from typing import Dict, List, Optional, Tuple

import portalocker


def get_folder_size(folder_path: str) -> int:
    """Total bytes under ``folder_path`` (recursive). Used for size reporting."""
    total = 0
    for dirpath, _dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
    return total


# ---------------------------------------------------------------------------
# Cache eviction
# ---------------------------------------------------------------------------
#
# Eviction operates on two kinds of units, collected in a single tree walk:
#   * "entry" -- a block-cache directory (one containing ``meta.bin``). Evicted
#     as a WHOLE via ``shutil.rmtree``; individual block files are never deleted
#     on their own, since that would leave ``meta.bin`` / ``chunks.crc`` behind
#     pointing at missing blocks (a corrupt entry). Before removing, a
#     non-blocking exclusive lock on the entry's ``meta.lock`` is attempted; if
#     it is held (a worker is mid open / create / resize), the entry is skipped
#     so eviction never races cache creation.
#   * "json" -- a loose cached API response under ``api/`` (``<name>.json`` plus
#     its ``<name>.body`` sidecar), evicted together.
#
# Ordering uses each unit's mtime (oldest first) for LRU/FIFO, or size (largest
# first) for LARGE_FIRST. mtime -- NOT atime -- drives LRU: atime is unreliable
# under noatime/relatime, and the old code bumped the *directory* atime which
# never propagated to the block files anyway. ``OlahCache.open`` bumps the
# entry's ``meta.lock`` mtime and ``read_cache_request`` bumps API-JSON mtime, so
# mtime is a faithful per-unit access signal.


def _ancestor_entry_root(dirpath: str, entry_roots: dict) -> Optional[str]:
    """Return the cache-entry root that owns ``dirpath`` (an ancestor with
    ``meta.bin``), or ``None`` if ``dirpath`` is not inside a cache entry."""
    cur = dirpath
    while True:
        if cur in entry_roots:
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def collect_cache_units(
    repos_path: str,
) -> Tuple[int, List[Dict]]:
    """Single-pass scan of the cache tree -> ``(total_bytes, units)``.

    Each unit is ``{"path", "size", "mtime", "kind"}`` (``kind`` is ``"entry"``
    or ``"json"``). Every regular file under ``repos_path`` contributes to
    ``total_bytes``; files inside a cache entry are attributed to that entry's
    unit, loose ``*.json`` API responses become ``"json"`` units (with their
    ``.body`` sidecar folded in), and ``*.json.body`` sidecars are not double
    counted.
    """
    # Pass 1: locate cache-entry roots (dirs containing meta.bin) + their mtimes.
    entry_roots: Dict[str, float] = {}
    for dirpath, _ds, filenames in os.walk(repos_path):
        if "meta.bin" in filenames:
            lock_path = os.path.join(dirpath, "meta.lock")
            try:
                if os.path.exists(lock_path):
                    mtime = os.stat(lock_path).st_mtime
                else:
                    mtime = os.stat(os.path.join(dirpath, "meta.bin")).st_mtime
            except OSError:
                mtime = 0.0
            entry_roots[dirpath] = mtime

    # Pass 2: attribute each file's size to its entry or to a loose JSON unit.
    total = 0
    entry_sizes: Dict[str, int] = {r: 0 for r in entry_roots}
    json_meta: Dict[str, List] = {}  # json_path -> [size, mtime]
    for dirpath, _ds, filenames in os.walk(repos_path):
        root = _ancestor_entry_root(dirpath, entry_roots)
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                st = os.stat(fp)
            except OSError:
                continue
            total += st.st_size
            if root is not None:
                entry_sizes[root] += st.st_size
                continue
            # Loose file outside any cache entry.
            if f.endswith(".json.body") and f[: -len(".body")] in filenames:
                # Sidecar of a sibling .json in the same dir; counted with it.
                continue
            if f.endswith(".json"):
                unit = json_meta.setdefault(fp, [0, st.st_mtime])
                unit[0] += st.st_size
                sidecar = fp + ".body"
                if os.path.exists(sidecar):
                    try:
                        unit[0] += os.stat(sidecar).st_size
                    except OSError:
                        pass

    units: List[Dict] = []
    for root, mtime in entry_roots.items():
        size = entry_sizes[root]
        if size > 0:
            units.append({"path": root, "size": size, "mtime": mtime, "kind": "entry"})
    for jp, (size, mtime) in json_meta.items():
        units.append({"path": jp, "size": size, "mtime": mtime, "kind": "json"})
    return total, units


def _try_evict_entry(entry_dir: str) -> bool:
    """Remove a whole cache entry unless its ``meta.lock`` is held (in use).

    Acquiring ``LOCK_EX | LOCK_NB`` on ``meta.lock`` succeeds only when no worker
    is mid open / create / resize; a streaming read holds block-file SH locks,
    not ``meta.lock``, so an in-flight read is safe (its open fds survive the
    rmtree on POSIX, and reads are resilient to a vanished block anyway).
    """
    lock_path = os.path.join(entry_dir, "meta.lock")
    try:
        fh = open(lock_path, "a+")
    except OSError:
        return False
    try:
        portalocker.lock(fh, portalocker.LOCK_EX | portalocker.LOCK_NB)
    except portalocker.LockException:
        fh.close()
        return False
    try:
        shutil.rmtree(entry_dir, ignore_errors=True)
        return not os.path.exists(entry_dir)
    finally:
        try:
            portalocker.unlock(fh)
        except Exception:
            pass
        try:
            fh.close()
        except Exception:
            pass


def _evict_json(json_path: str) -> None:
    for p in (json_path, json_path + ".body"):
        try:
            os.remove(p)
        except OSError:
            pass


def evict_cache_to_limit(repos_path: str, limit: int, strategy: str = "LRU") -> int:
    """Evict whole cache entries + loose API JSON until ``total <= limit``.

    Returns the post-eviction total byte count (best-effort; concurrent writers
    may change it between collection and removal). Never evicts individual block
    files. See the module docstring for the full rationale.
    """
    total, units = collect_cache_units(repos_path)
    if total <= limit:
        return total
    if strategy == "LARGE_FIRST":
        units.sort(key=lambda u: u["size"], reverse=True)
    else:
        # LRU and FIFO both order by mtime asc (oldest first); FIFO approximates
        # write/creation order via mtime, which is the closest reliable signal.
        units.sort(key=lambda u: u["mtime"])
    for u in units:
        if total <= limit:
            break
        if u["kind"] == "entry":
            if _try_evict_entry(u["path"]):
                total -= u["size"]
        else:
            _evict_json(u["path"])
            total -= u["size"]
    return total


def convert_to_bytes(size_str) -> Optional[int]:
    if size_str is None:
        return None
    if isinstance(size_str, int):
        return size_str
    size_str = str(size_str).strip().upper()
    multipliers = {
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }

    for unit in multipliers:
        if size_str.endswith(unit):
            size = int(size_str[: -len(unit)])
            return size * multipliers[unit]

    # Default use bytes
    try:
        return int(size_str)
    except ValueError:
        return None


def convert_bytes_to_human_readable(bytes: int) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while bytes >= 1024 and index < len(suffixes) - 1:
        bytes /= 1024
        index += 1
    return f"{bytes:.2f} {suffixes[index]}"
