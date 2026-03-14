import io
import json

import pytest

from olah.cache.bitset import Bitset
from olah import errors
from olah.mirror.meta import RepoMeta

pytest.importorskip("portalocker")

from olah.cache.olah_cache import CURRENT_OLAH_CACHE_VERSION, MAX_BLOCK_NUM, OlahCache, OlahCacheHeader


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
