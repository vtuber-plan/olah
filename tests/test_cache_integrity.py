import pytest

from olah.cache.integrity import lfs_sha256_from_pathsinfo, normalize_sha256, verify_cache_sha256
from olah.cache.olah_cache import OlahCache

GOOD = "f3668ba4cccf1ca6a7eb84e888fb92c1cdc7204d472ba9db771e6fd3abf6b874"
BAD = "1e639b310945b23a88919e0fe96624bc64f30579b28a6f862022e49a4b00ce3e"


def test_normalize_sha256_accepts_hex_and_rejects_garbage():
    assert normalize_sha256(GOOD) == GOOD
    assert normalize_sha256(GOOD.upper()) == GOOD
    assert normalize_sha256("not-a-hash") is None


def test_lfs_sha256_from_pathsinfo_reads_lfs_oid():
    row = {
        "path": "model-00001-of-00048.safetensors",
        "size": 1059061856,
        "lfs": {"oid": GOOD, "size": 1059061856},
    }
    assert lfs_sha256_from_pathsinfo(row) == GOOD
    assert lfs_sha256_from_pathsinfo({"size": 1}) is None


@pytest.mark.asyncio
async def test_content_sha256_hashes_terminal_block_without_padding(tmp_path):
    cache = OlahCache.create(str(tmp_path / "cache"))
    payload = b"hello-olah-cache!!xx"
    assert len(payload) == 20
    cache.resize(len(payload))
    block = payload + b"\x00" * (cache._get_block_size() - len(payload))
    await cache.write_block(0, block)

    assert await cache.content_sha256() == __import__("hashlib").sha256(payload).hexdigest()
    cache.close()


@pytest.mark.asyncio
async def test_verify_cache_sha256_invalidates_on_mismatch(tmp_path):
    cache = OlahCache.create(str(tmp_path / "cache"))
    payload = b"hello-olah-cache!!xx"
    assert len(payload) == 20
    cache.resize(len(payload))
    block = payload + b"\x00" * (cache._get_block_size() - len(payload))
    await cache.write_block(0, block)

    ok = await verify_cache_sha256(cache, GOOD, save_path=str(tmp_path / "cache"))
    assert ok is False
    assert cache.has_block(0) is False
    cache.close()


@pytest.mark.asyncio
async def test_verify_cache_sha256_passes_when_hash_matches(tmp_path):
    cache = OlahCache.create(str(tmp_path / "cache"))
    payload = b"hello-olah-cache!!xx"
    assert len(payload) == 20
    cache.resize(len(payload))
    block = payload + b"\x00" * (cache._get_block_size() - len(payload))
    await cache.write_block(0, block)
    expected = __import__("hashlib").sha256(payload).hexdigest()

    ok = await verify_cache_sha256(cache, expected, save_path=str(tmp_path / "cache"))
    assert ok is True
    assert cache.has_block(0) is True
    cache.close()
