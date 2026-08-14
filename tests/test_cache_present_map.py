# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for the v10 in-memory presence bitmap and resilient reads.

The bitmap (revived ``cache/bitset.py``) is built once per ``open()`` from a
single ``blocks/`` scan and kept in sync with writes/invalidations, so bulk
iterators avoid a stat-per-block. ``read_block``/``stream_range`` must also
tolerate a block vanishing mid-request (whole-entry eviction) without surfacing
an opaque 500.
"""

import asyncio
import os

import pytest

pytest.importorskip("portalocker")

from olah.cache.olah_cache import CacheIntegrityError, OlahCache  # noqa: E402

BLOCK = 64


def _new_cache(tmp_path, name, payload, etag):
    sp = os.path.join(str(tmp_path), name)
    c = OlahCache(sp, file_size=len(payload), block_size=BLOCK,
                  chunk_size=BLOCK, expected_etag=etag)
    return c, sp


def _fill(c, payload):
    async def go():
        n = (len(payload) + BLOCK - 1) // BLOCK
        for i in range(n):
            chunk = payload[i * BLOCK:(i + 1) * BLOCK]
            await c.write_block(i, chunk + b"\x00" * (BLOCK - len(chunk)))

    asyncio.run(go())


def test_bitmap_agrees_with_disk_after_write_and_invalidate(tmp_path):
    payload = bytes((i * 7) % 256 for i in range(150))  # 3 blocks (last partial)
    c, _ = _new_cache(tmp_path, "c", payload, "etag-a")
    _fill(c, payload)

    # Bitmap mirrors the authoritative has_block() for every block.
    assert [c.is_block_cached(i) for i in range(3)] == [True, True, True]
    assert c.is_fully_cached()
    assert [c.has_block(i) for i in range(3)] == [c.is_block_cached(i) for i in range(3)]

    asyncio.run(c._invalidate_block(1))
    assert c.is_block_cached(1) is False
    assert c.is_fully_cached() is False
    c.close()


def test_bitmap_is_rebuilt_from_disk_on_reopen(tmp_path):
    payload = bytes((i * 7) % 256 for i in range(150))
    c, sp = _new_cache(tmp_path, "c", payload, "etag-a")
    _fill(c, payload)
    asyncio.run(c._invalidate_block(0))
    c.close()

    c2 = OlahCache(sp, expected_etag="etag-a")
    try:
        # Rebuilt from a fresh blocks/ scan, independent of the prior instance.
        assert c2.is_block_cached(0) is False
        assert c2.is_block_cached(1) is True
        assert c2.is_fully_cached() is False
    finally:
        c2.close()


def test_bitmap_reflects_new_blocks_after_resize(tmp_path):
    payload = bytes((i * 7) % 256 for i in range(64))  # 1 block
    c, _ = _new_cache(tmp_path, "c", payload, "etag-a")
    _fill(c, payload)
    assert c.is_fully_cached()

    c.resize(3 * BLOCK)  # grow to 3 blocks
    try:
        # Block 0 still present; blocks 1, 2 are new and absent.
        assert c.is_block_cached(0) is True
        assert c.is_block_cached(1) is False
        assert c.is_block_cached(2) is False
        assert c.is_fully_cached() is False
    finally:
        c.close()


def test_read_block_returns_none_for_block_removed_mid_request(tmp_path):
    """A block evicted between has_block() and the read yields None, not a raise."""
    payload = bytes((i * 7) % 256 for i in range(150))
    c, sp = _new_cache(tmp_path, "c", payload, "etag-a")
    _fill(c, payload)
    c.close()

    reader = OlahCache(sp, expected_etag="etag-a")
    try:
        # Simulate whole-entry eviction removing one block out from under us.
        os.remove(os.path.join(sp, "blocks", "block_00000001.bin"))
        assert asyncio.run(reader.read_block(1)) is None
    finally:
        reader.close()


def test_stream_range_raises_typed_error_for_vanished_block(tmp_path):
    """A missing block mid-stream surfaces CacheIntegrityError (recoverable),
    not the old opaque 'read block which has not been cached' 500."""
    payload = bytes((i * 7) % 256 for i in range(150))
    c, sp = _new_cache(tmp_path, "c", payload, "etag-a")
    _fill(c, payload)
    c.close()

    reader = OlahCache(sp, expected_etag="etag-a")
    try:
        os.remove(os.path.join(sp, "blocks", "block_00000001.bin"))
        with pytest.raises(CacheIntegrityError):
            asyncio.run(_collect(reader.stream_range(0, len(payload))))
    finally:
        reader.close()


async def _collect(gen):
    return b"".join([x async for x in gen])
