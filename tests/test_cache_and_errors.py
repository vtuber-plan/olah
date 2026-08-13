import io
import json
import os
import struct

import pytest

from olah.cache.bitset import Bitset
from olah import errors
from olah.mirror.meta import RepoMeta

pytest.importorskip("portalocker")

from olah.cache.olah_cache import (
    COMPRESSION_NAME_TO_ALGO,
    CURRENT_OLAH_CACHE_VERSION,
    MAX_BLOCK_NUM,
    CacheIntegrityError,
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
        chunk_size=512,
        etag='"deadbeef"',
        extension=b"\x01\x00\x00\x00extra",
    )
    stream = io.BytesIO()

    header.write(stream)
    stream.seek(0)
    restored = OlahCacheHeader.read(stream)

    assert restored.version == CURRENT_OLAH_CACHE_VERSION
    assert restored.block_size == 1024
    assert restored.file_size == 2049
    assert restored.chunk_size == 512
    assert restored.block_number == 3
    assert restored.chunk_number == 5
    assert restored.chunks_per_block() == 2
    assert restored.compression_algo == 2
    assert restored.etag == b'"deadbeef"'
    assert restored.extension == b"\x01\x00\x00\x00extra"


def test_olah_cache_header_detects_corruption_and_oversized_files():
    with pytest.raises(Exception, match="not a Olah cache file"):
        OlahCacheHeader.read(io.BytesIO(b"BAD!"))

    # A valid header whose body is tampered after writing must fail the CRC.
    header = OlahCacheHeader(block_size=512, file_size=10, chunk_size=512, etag="t")
    stream = io.BytesIO()
    header.write(stream)
    data = bytearray(stream.getvalue())
    data[-5] ^= 0xFF  # flip a byte inside the CRC-covered region
    with pytest.raises(Exception, match="CRC mismatch"):
        OlahCacheHeader.read(io.BytesIO(bytes(data)))

    oversized = OlahCacheHeader(
        version=CURRENT_OLAH_CACHE_VERSION,
        block_size=1,
        chunk_size=1,
        file_size=MAX_BLOCK_NUM + 1,
    )
    with pytest.raises(Exception, match="out of the max capability"):
        oversized._validate()

    # block_size must be a multiple of chunk_size.
    with pytest.raises(Exception, match="multiple of chunk_size"):
        OlahCacheHeader(block_size=5, chunk_size=2)._validate()


def test_compression_algo_from_name_maps_known_names():
    assert compression_algo_from_name("none") == 0
    assert compression_algo_from_name("gzip") == 1
    assert compression_algo_from_name("lzma") == 2
    assert COMPRESSION_NAME_TO_ALGO == {"none": 0, "gzip": 1, "lzma": 2}
    with pytest.raises(ValueError):
        compression_algo_from_name("brotli")


def _full_block(prefix: bytes, block_size: int) -> bytes:
    return prefix + b"\x00" * (block_size - len(prefix))


# Small geometry used by the round-trip tests for speed: block 64 B, chunk 16 B.
_BS, _CS = 64, 16


@pytest.mark.asyncio
async def test_olah_cache_round_trips_uncompressed_block(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(
        str(cache_dir), file_size=_BS, block_size=_BS, chunk_size=_CS, compression_algo=0
    )
    await cache.write_block(0, _full_block(b"abcd", _BS))
    cache.close()

    cache = OlahCache(str(cache_dir))
    assert cache.header.compression_algo == 0
    block = await cache.read_block(0)
    cache.close()

    assert block[:4] == b"abcd"
    assert len(block) == _BS


@pytest.mark.asyncio
async def test_olah_cache_round_trips_compressed_blocks(tmp_path):
    # Both gzip and lzma caches must round-trip, including incompressible data
    # (model-weight-like). Each sub-chunk is independently decompressed.
    payload = os.urandom(_BS)
    for algo in (1, 2):
        cache_dir = tmp_path / f"cache{algo}"
        cache = OlahCache.create(
            str(cache_dir),
            file_size=_BS,
            block_size=_BS,
            chunk_size=_CS,
            compression_algo=algo,
        )
        await cache.write_block(0, payload)
        cache.close()

        cache = OlahCache(str(cache_dir))
        assert cache.header.compression_algo == algo
        assert (await cache.read_block(0)) == payload
        cache.close()


@pytest.mark.asyncio
async def test_olah_cache_compressed_random_access_range(tmp_path):
    # The compressed random-access fix: stream_range decompresses only the
    # overlapping sub-chunks and returns the exact requested bytes.
    payload = bytes((i * 7 + 1) & 0xFF for i in range(_BS * 3))  # 3 blocks
    for algo in (1, 2):
        cache_dir = tmp_path / f"cache{algo}"
        cache = OlahCache.create(
            str(cache_dir),
            file_size=len(payload),
            block_size=_BS,
            chunk_size=_CS,
            compression_algo=algo,
        )
        for bi in range(3):
            chunk = payload[bi * _BS : (bi + 1) * _BS]
            await cache.write_block(bi, chunk)
        cache.close()

        cache = OlahCache(str(cache_dir))
        out = b""
        async for piece in cache.stream_range(5, 2 * _BS + 9):
            out += piece
        cache.close()
        assert out == payload[5 : 2 * _BS + 9], (algo, out)


@pytest.mark.asyncio
async def test_olah_cache_corrupt_chunk_is_dropped_not_served(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(
        str(cache_dir), file_size=_BS, block_size=_BS, chunk_size=_CS, compression_algo=0
    )
    await cache.write_block(0, _full_block(b"abcd", _BS))
    cache.close()

    # Flip a byte inside the (uncompressed) block file -> CRC mismatch on read.
    cache = OlahCache(str(cache_dir))
    with open(cache.get_block_path(0), "r+b") as fh:
        fh.seek(2)
        fh.write(bytes([0xFF]))
    with pytest.raises(CacheIntegrityError):
        async for _ in cache.stream_range(0, _BS):
            pass
    assert cache.has_block(0) is False  # corrupt block invalidated
    cache.close()


@pytest.mark.asyncio
async def test_olah_cache_chunks_crc_written_and_verified(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(
        str(cache_dir), file_size=_BS, block_size=_BS, chunk_size=_CS, compression_algo=0
    )
    await cache.write_block(0, _full_block(b"abcd", _BS))
    # chunks.crc exists and has one slot per chunk (4 chunks -> 16 bytes).
    assert os.path.getsize(cache_dir / "chunks.crc") == 4 * 4
    crcs = cache._read_chunk_crcs(0)
    assert crcs is not None and len(crcs) == 4 and all(c != 0 for c in crcs)
    cache.close()


@pytest.mark.asyncio
async def test_olah_cache_etag_mismatch_online_recreates(tmp_path):
    cache_dir = tmp_path / "cache"
    c = OlahCache(str(cache_dir), file_size=8, block_size=8, chunk_size=8, expected_etag="AAA")
    assert c.header.etag == b"AAA"
    c.close()

    # Online revalidation with a different etag wipes & recreates.
    c2 = OlahCache(str(cache_dir), file_size=8, block_size=8, chunk_size=8, expected_etag="BBB")
    assert c2.header.etag == b"BBB"
    c2.close()


@pytest.mark.asyncio
async def test_olah_cache_offline_does_not_destroy_on_reopen(tmp_path):
    cache_dir = tmp_path / "cache"
    OlahCache(str(cache_dir), file_size=8, block_size=8, chunk_size=8, expected_etag="AAA").close()
    # Offline callers pass expected_etag=None -> the on-disk etag is trusted.
    c = OlahCache(str(cache_dir), file_size=8, block_size=8, chunk_size=8, expected_etag=None)
    assert c.header.etag == b"AAA"
    c.close()


@pytest.mark.asyncio
async def test_olah_cache_wipes_legacy_version(tmp_path):
    cache_dir = tmp_path / "cache"
    blocks_dir = cache_dir / "blocks"
    blocks_dir.mkdir(parents=True)
    # Hand-write a legacy v9 meta.bin (36-byte fixed header).
    with open(cache_dir / "meta.bin", "wb") as f:
        f.write(struct.pack("<4sQQQQ", b"OLAH", 9, 8, 8, 0))
    # Opening must wipe + recreate at the current version.
    c = OlahCache(str(cache_dir), file_size=8, block_size=8, chunk_size=8, expected_etag=None)
    assert c.header.version == CURRENT_OLAH_CACHE_VERSION
    c.close()


@pytest.mark.asyncio
async def test_olah_cache_read_only_hit_does_not_rewrite_meta_bin(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(
        str(cache_dir), file_size=_BS, block_size=_BS, chunk_size=_CS, compression_algo=0
    )
    await cache.write_block(0, _full_block(b"abcd", _BS))
    cache.close()

    meta_path = cache_dir / "meta.bin"
    fixed_mtime = 1234567.0
    os.utime(meta_path, (fixed_mtime, fixed_mtime))

    cache = OlahCache(str(cache_dir))
    block = await cache.read_block(0)
    cache.close()

    assert block[:4] == b"abcd"
    assert os.path.getmtime(meta_path) == fixed_mtime


@pytest.mark.asyncio
async def test_olah_cache_ignores_zero_length_block_placeholders(tmp_path):
    cache = OlahCache.create(
        str(tmp_path / "cache"), file_size=_BS, block_size=_BS, chunk_size=_CS
    )
    empty_block = tmp_path / "cache" / "blocks" / "block_00000000.bin"
    empty_block.write_bytes(b"")
    assert cache.has_block(0) is False
    await cache.write_block(0, _full_block(b"abcd", _BS))
    assert cache.has_block(0) is True
    assert empty_block.stat().st_size > 0
    cache.close()


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


@pytest.mark.asyncio
async def test_olah_cache_is_fully_cached(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = OlahCache.create(
        str(cache_dir), file_size=_BS * 2, block_size=_BS, chunk_size=_CS, compression_algo=0
    )
    # No blocks yet -> not fully cached; zero-size is also not "fully cached".
    assert cache.is_fully_cached() is False
    await cache.write_block(0, _full_block(b"abcd", _BS))
    assert cache.is_fully_cached() is False  # block 1 still missing
    await cache.write_block(1, _full_block(b"efgh", _BS))
    assert cache.is_fully_cached() is True
    cache.close()

    # Reopen: still detected as fully cached from disk state.
    cache = OlahCache(str(cache_dir))
    assert cache.is_fully_cached() is True
    cache.close()
