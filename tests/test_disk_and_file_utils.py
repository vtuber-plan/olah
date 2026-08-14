import asyncio
import os

import portalocker
import pytest

from olah.utils.disk_utils import (
    collect_cache_units,
    convert_bytes_to_human_readable,
    convert_to_bytes,
    evict_cache_to_limit,
    get_folder_size,
)
from olah.utils.file_utils import make_dirs

# Eviction correctness depends on real fcntl.flock mutual exclusion.
pytest.importorskip("portalocker")

from olah.cache.olah_cache import OlahCache  # noqa: E402
from olah.utils.cache_utils import write_cache_request  # noqa: E402


def test_make_dirs_creates_parent_directories_for_file_paths(tmp_path):
    target = tmp_path / "nested" / "path" / "file.txt"
    make_dirs(str(target))
    assert target.parent.is_dir()


def test_get_folder_size_sums_nested_files(tmp_path):
    small = tmp_path / "small.txt"
    medium = tmp_path / "nested" / "medium.txt"
    make_dirs(str(medium))
    small.write_bytes(b"a")
    medium.write_bytes(b"bb")
    assert get_folder_size(str(tmp_path)) == 3


def test_convert_size_helpers_handle_supported_units_and_invalid_values():
    assert convert_to_bytes("128") == 128
    assert convert_to_bytes("2KB") == 2 * 1024
    assert convert_to_bytes("3 mb") == 3 * 1024**2
    assert convert_to_bytes(None) is None
    assert convert_to_bytes(4096) == 4096
    assert convert_to_bytes("invalid") is None

    assert convert_bytes_to_human_readable(512) == "512.00 B"
    assert convert_bytes_to_human_readable(2048) == "2.00 KB"


# ---------------------------------------------------------------------------
# Eviction
# ---------------------------------------------------------------------------

def _make_entry(repos_path: str, name: str, payload: bytes, block_size: int, etag: str) -> str:
    """Create a populated block-cache entry under <repos>/files/<name>."""
    sp = os.path.join(repos_path, "files", name)
    c = OlahCache(
        sp, file_size=len(payload), block_size=block_size,
        chunk_size=block_size, expected_etag=etag,
    )

    async def _fill():
        for i in range((len(payload) + block_size - 1) // block_size):
            chunk = payload[i * block_size:(i + 1) * block_size]
            await c.write_block(i, chunk + b"\x00" * (block_size - len(chunk)))

    asyncio.run(_fill())
    c.close()
    return sp


def test_collect_cache_units_attributes_entries_and_json_sidecars(tmp_path):
    repos = str(tmp_path / "repos")
    payload = bytes((i * 7) % 256 for i in range(64))
    _make_entry(repos, "a", payload, 64, "etag-a")

    jp = os.path.join(repos, "api", "models", "o", "r", "revision", "main", "meta_get.json")
    asyncio.run(write_cache_request(jp, 200, {"x": "y"}, b"body-bytes!"))

    total, units = collect_cache_units(repos)
    entries = [u for u in units if u["kind"] == "entry"]
    jsons = [u for u in units if u["kind"] == "json"]
    assert len(entries) == 1
    assert len(jsons) == 1
    # The JSON unit folds in its .body sidecar (not double-counted as its own unit).
    assert jsons[0]["size"] == os.path.getsize(jp) + os.path.getsize(jp + ".body")
    assert entries[0]["size"] > 0


def test_evict_removes_whole_oldest_entry_and_keeps_newer(tmp_path):
    repos = str(tmp_path / "repos")
    payload = bytes((i * 7) % 256 for i in range(64))
    sp_old = _make_entry(repos, "old", payload, 64, "etag-old")
    sp_new = _make_entry(repos, "new", payload, 64, "etag-new")
    # meta.lock mtime is the LRU signal; force a deterministic order.
    os.utime(os.path.join(sp_old, "meta.lock"), (1000, 1000))
    os.utime(os.path.join(sp_new, "meta.lock"), (2000, 2000))

    _, units = collect_cache_units(repos)
    new_size = next(u["size"] for u in units if u["path"] == sp_new)
    # Limit = newer entry's size: the older entry must go, the newer stays.
    evict_cache_to_limit(repos, new_size, "LRU")

    assert not os.path.exists(sp_old), "oldest entry must be removed wholesale"
    assert not os.path.exists(os.path.join(sp_old, "meta.bin")), "no stray meta.bin"
    assert os.path.exists(sp_new), "newer entry must remain"


def test_evict_skips_entry_whose_meta_lock_is_held(tmp_path):
    repos = str(tmp_path / "repos")
    payload = bytes((i * 7) % 256 for i in range(64))
    sp = _make_entry(repos, "held", payload, 64, "etag-held")

    # Hold a lock on meta.lock as a worker mid open/create/resize would.
    fh = open(os.path.join(sp, "meta.lock"), "a+")
    portalocker.lock(fh, portalocker.LOCK_SH | portalocker.LOCK_NB)
    try:
        evict_cache_to_limit(repos, 0, "LRU")  # would otherwise evict everything
        assert os.path.exists(sp), "an entry whose meta.lock is held must not be evicted"
    finally:
        portalocker.unlock(fh)
        fh.close()


def test_evict_removes_api_json_and_its_body_sidecar(tmp_path):
    repos = str(tmp_path / "repos")
    jp = os.path.join(repos, "api", "models", "o", "r", "revision", "main", "meta_get.json")
    asyncio.run(write_cache_request(jp, 200, {"x": "y"}, b"body-bytes"))
    assert os.path.exists(jp) and os.path.exists(jp + ".body")

    evict_cache_to_limit(repos, 0, "LRU")

    assert not os.path.exists(jp)
    assert not os.path.exists(jp + ".body")
