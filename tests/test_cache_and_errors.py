import io
import json
import os

import pytest

from olah.cache.bitset import Bitset
from olah import errors
from olah.mirror.meta import RepoMeta

pytest.importorskip("portalocker")

from olah.cache.olah_cache import (
    COMPRESSION_NAME_TO_ALGO,
    CURRENT_OLAH_CACHE_VERSION,
    MAX_BLOCK_NUM,
    OlahCache,
    OlahCacheHeader,
    compression_algo_from_name,
)


def test_bitset_can_set_clear_and_validate_bounds():
    bitset = Bitset(10)

    bitset.set(1)
    bitset.set(9)
    bitset.clear(1)

    assert bitset.test(1) is False
    assert bitset.test(9) is True
    assert str(bitset).startswith("0000000001")

    with pytest.raises(IndexError):
        bitset.set(10)


def test_olah_cache_header_round_trips_through_binary_stream():
    header = OlahCacheHeader(
        version=CURRENT_OLAH_CACHE_VERSION,
        block_size=1024,
        file_size=2049,
        compression_algo=2,
    )
    stream = io.BytesIO()

    header.write(stream)
    stream.seek(0)
    restored = OlahCacheHeader.read(stream)

    assert restored.version == CURRENT_OLAH_CACHE_VERSION
    assert restored.block_size == 1024
    assert restored.file_size == 2049
    assert restored.block_number == 3
    assert restored.compression_algo == 2


def test_olah_cache_header_rejects_invalid_magic_and_oversized_files():
    with pytest.raises(Exception, match="not a Olah cache file"):
        OlahCacheHeader.read(io.BytesIO(b"BAD!"))

    oversized = OlahCacheHeader(
        version=CURRENT_OLAH_CACHE_VERSION,
        block_size=1,
        file_size=MAX_BLOCK_NUM + 1,
    )
    with pytest.raises(Exception, match="out of the max capability"):
        oversized._valid_header()


@pytest.mark.asyncio
async def test_olah_cache_ignores_zero_length_block_placeholders(tmp_path):
    cache = OlahCache.create(str(tmp_path / "cache"))
    cache.resize(16)

    empty_block = tmp_path / "cache" / "blocks" / "block_00000000.bin"
    empty_block.write_bytes(b"")

    assert cache.has_block(0) is False

    payload = b"abcd" + b"\x00" * (cache._get_block_size() - 4)
    await cache.write_block(0, payload)

    assert cache.has_block(0) is True
    assert empty_block.stat().st_size > 0
    cache.close()


def test_compression_algo_from_name_maps_known_names():
    assert compression_algo_from_name("none") == 0
    assert compression_algo_from_name("gzip") == 1
    assert compression_algo_from_name("lzma") == 2
    assert COMPRESSION_NAME_TO_ALGO == {"none": 0, "gzip": 1, "lzma": 2}
    with pytest.raises(ValueError):
        compression_algo_from_name("brotli")


def _full_block(prefix: bytes, block_size: int) -> bytes:
    return prefix + b"\x00" * (block_size - len(prefix))


@pytest.mark.asyncio
async def test_olah_cache_round_trips_uncompressed_block(tmp_path):
    block_size = 64
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(str(cache_dir), block_size=block_size, compression_algo=0)
    cache.resize(block_size)
    await cache.write_block(0, _full_block(b"abcd", block_size))
    cache.close()

    # Reopen the existing cache and read the block back. Exercises the
    # single-open read path and confirms algo=0 stores/restores raw bytes.
    cache = OlahCache(str(cache_dir))
    assert cache.header.compression_algo == 0
    block = await cache.read_block(0)
    cache.close()

    assert block[:4] == b"abcd"
    assert len(block) == block_size


@pytest.mark.asyncio
async def test_olah_cache_round_trips_gzip_block(tmp_path):
    # Backward-compat: gzip caches (the previous default) must still round-trip.
    block_size = 64
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(str(cache_dir), block_size=block_size, compression_algo=1)
    cache.resize(block_size)
    await cache.write_block(0, _full_block(b"wxyz", block_size))
    cache.close()

    cache = OlahCache(str(cache_dir))
    assert cache.header.compression_algo == 1
    block = await cache.read_block(0)
    cache.close()

    assert block[:4] == b"wxyz"
    assert len(block) == block_size


@pytest.mark.asyncio
async def test_olah_cache_gzip_round_trips_incompressible_block(tmp_path):
    # Incompressible data (like model weights) compresses to slightly MORE than
    # its original size because of gzip framing. read_block must read the whole
    # block file rather than capping at block_size, otherwise the gzip stream is
    # truncated and decompression raises EOFError.
    block_size = 64
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(str(cache_dir), block_size=block_size, compression_algo=1)
    cache.resize(block_size)
    payload = os.urandom(block_size)
    await cache.write_block(0, payload)
    cache.close()

    cache = OlahCache(str(cache_dir))
    block = await cache.read_block(0)
    cache.close()

    assert block == payload


@pytest.mark.asyncio
async def test_olah_cache_read_only_hit_does_not_rewrite_meta_bin(tmp_path):
    block_size = 64
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(str(cache_dir), block_size=block_size, compression_algo=0)
    cache.resize(block_size)
    await cache.write_block(0, _full_block(b"abcd", block_size))
    cache.close()

    meta_path = cache_dir / "meta.bin"
    # Pin mtime to a fixed value: a read-only reopen must leave meta.bin
    # untouched. Byte-equality alone would not catch a redundant identical
    # rewrite; the mtime pin does, deterministically.
    fixed_mtime = 1234567.0
    os.utime(meta_path, (fixed_mtime, fixed_mtime))

    # Read-only reopen: no resize, no write_block -> close must NOT flush.
    cache = OlahCache(str(cache_dir))
    block = await cache.read_block(0)
    cache.close()

    assert block[:4] == b"abcd"
    assert os.path.getmtime(meta_path) == fixed_mtime


def test_error_responses_return_expected_status_and_headers():
    repo_missing = errors.error_repo_not_found()
    revision_missing = errors.error_revision_not_found("abc123")
    proxy_timeout = errors.error_proxy_timeout()

    assert repo_missing.status_code == 401
    assert repo_missing.headers["x-error-code"] == "RepoNotFound"
    assert json.loads(repo_missing.body) == {"error": "Repository not found"}
    assert revision_missing.status_code == 404
    assert json.loads(revision_missing.body) == {"error": "Invalid rev id: abc123"}
    assert proxy_timeout.status_code == 504
    assert proxy_timeout.headers["x-error-message"] == "Proxy Timeout"


def test_repo_meta_to_dict_exposes_current_field_values():
    meta = RepoMeta()
    meta._id = "internal-id"
    meta.id = "team/demo"
    meta.author = "team"
    meta.tags = ["featured"]
    meta.likes = 7

    assert meta.to_dict()["id"] == "team/demo"
    assert meta.to_dict()["_id"] == "internal-id"
    assert meta.to_dict()["tags"] == ["featured"]
    assert meta.to_dict()["likes"] == 7
