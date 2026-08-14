# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Concurrency tests for the v10 block cache.

These exercise the four cross-process / cross-coroutine hardening fixes:

1. ``meta.lock`` readers-writer lifecycle -- concurrent opens of a stale cache
   produce exactly ONE create.
2. Per-block single-flight -- N concurrent requests for the same uncached file
   download each block exactly once.
3. Leader-death recovery -- a leader that fails mid-download is retried by a
   follower.
4. ``resize`` under the exclusive meta lock -- concurrent resizes never leave a
   torn header / crc-file combo.

They require a REAL ``portalocker`` (the codebase uses ``fcntl.flock``, which
excludes across both threads and processes), so the module is skipped when
portalocker is absent.
"""

import asyncio
import os
import threading
import time
from types import SimpleNamespace

import pytest

pytest.importorskip("portalocker")

from olah.cache.olah_cache import (  # noqa: E402
    CURRENT_OLAH_CACHE_VERSION,
    OlahCache,
    OlahCacheHeader,
)
from olah.proxy import files as proxy_files  # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_app(tmp_path, block_size=None, chunk_size=None):
    config = SimpleNamespace(
        offline=False,
        hf_netloc="huggingface.co",
        hf_lfs_netloc="cdn-lfs.huggingface.co",
        repos_path=str(tmp_path / "repos"),
        hf_url_base=lambda: "https://huggingface.co",
        hf_lfs_url_base=lambda: "https://cdn-lfs.huggingface.co",
        cache_compression="none",
        cache_block_size=block_size,
        cache_chunk_size=chunk_size,
    )
    return SimpleNamespace(
        state=SimpleNamespace(app_settings=SimpleNamespace(config=config))
    )


class _FakeStreamResponse:
    """Mimics the slice of httpx's streaming response that the downloader reads."""

    def __init__(self, body: bytes, status_code: int = 206):
        self.status_code = status_code
        self.headers = {"content-length": str(len(body))}
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def aiter_raw(self):
        if not self._body:
            return
        mid = max(1, len(self._body) // 2)
        yield self._body[:mid]
        yield self._body[mid:]


class _RangeSliceClient:
    """Counts upstream ``stream()`` calls and serves the requested byte range.

    Range-aware so it stays correct under per-block fetching (each block is a
    separate ``bytes=`` range request). Optional ``fail_first_n`` simulates a
    leader that dies before publishing its first ``fail_first_n`` downloads.
    """

    def __init__(self, payload: bytes, fail_first_n: int = 0):
        self.payload = payload
        self.calls = 0
        self._fail_first_n = fail_first_n
        self._lock = threading.Lock()

    def stream(self, **kwargs):
        headers = kwargs.get("headers") or {}
        rng = headers.get("range", "")
        if rng.startswith("bytes="):
            s, _, e = rng[6:].partition("-")
            body = self.payload[int(s): int(e) + 1 if e else len(self.payload)]
        else:
            body = self.payload
        with self._lock:
            self.calls += 1
            call_n = self.calls
        if call_n <= self._fail_first_n:
            raise RuntimeError("simulated leader death before publish")
        return _FakeStreamResponse(body)


async def _collect(gen):
    return b"".join([c async for c in gen])


# ---------------------------------------------------------------------------
# 1. meta.lock serializes creation
# ---------------------------------------------------------------------------

def test_concurrent_open_serializes_cache_creation(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    # Seed a corrupt meta.bin so every open must wipe + create.
    (cache_dir / "meta.bin").write_bytes(b"NOT_AN_OLAH_CACHE_HEADER")

    flush_count = {"n": 0}
    count_lock = threading.Lock()
    orig_flush = OlahCache._flush_header

    def counting_flush(self):
        with count_lock:
            flush_count["n"] += 1
        return orig_flush(self)

    errors = []

    def worker():
        try:
            c = OlahCache(
                str(cache_dir),
                file_size=1024,
                block_size=64,
                chunk_size=64,
                expected_etag="abc",
            )
            c.close()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    OlahCache._flush_header = counting_flush
    try:
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        OlahCache._flush_header = orig_flush

    assert not errors
    # Despite 8 concurrent opens on a corrupt cache, exactly one create ran.
    assert flush_count["n"] == 1, f"expected 1 create, got {flush_count['n']}"
    # And the resulting cache is valid and reusable.
    c = OlahCache(str(cache_dir), expected_etag="abc")
    try:
        assert c.header.version == CURRENT_OLAH_CACHE_VERSION
        assert c.header.etag == b"abc"
    finally:
        c.close()


# ---------------------------------------------------------------------------
# 2. per-block single-flight dedups concurrent downloads
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_flight_dedups_concurrent_block_downloads(tmp_path):
    block_size = 64
    payload = bytes((i * 13) % 256 for i in range(150))  # 3 blocks, last partial
    num_blocks = (len(payload) + block_size - 1) // block_size
    save_path = tmp_path / "cache"

    client = _RangeSliceClient(payload)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    async def fetch():
        return await _collect(
            proxy_files._file_chunk_get(
                app=app,
                save_path=str(save_path),
                client=client,
                method="GET",
                url="https://huggingface.co/team/demo/resolve/main/blob.bin",
                headers={},
                allow_cache=True,
                file_size=len(payload),
            )
        )

    n = 4
    results = await asyncio.gather(*(fetch() for _ in range(n)))

    # Every caller got the full correct file...
    assert all(r == payload for r in results)
    # ...and each block was downloaded exactly once (not n * num_blocks).
    assert client.calls == num_blocks, (
        f"expected {num_blocks} upstream downloads (single-flight), got {client.calls}"
    )
    # ...and the cache is now fully populated.
    c = OlahCache(str(save_path))
    try:
        assert c.is_fully_cached()
    finally:
        c.close()


# ---------------------------------------------------------------------------
# 3. leader-death recovery -- a failed leader is retried by a follower
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_flight_recovers_when_leader_dies(tmp_path):
    block_size = 64
    payload = bytes((i * 5) % 256 for i in range(70))  # 2 blocks
    num_blocks = (len(payload) + block_size - 1) // block_size
    save_path = tmp_path / "cache"

    # The first upstream call (the first leader's download) fails; the follower
    # that takes over succeeds.
    client = _RangeSliceClient(payload, fail_first_n=1)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    async def fetch():
        return await _collect(
            proxy_files._file_chunk_get(
                app=app,
                save_path=str(save_path),
                client=client,
                method="GET",
                url="https://huggingface.co/team/demo/resolve/main/blob.bin",
                headers={},
                allow_cache=True,
                file_size=len(payload),
            )
        )

    results = await asyncio.gather(*(fetch() for _ in range(2)), return_exceptions=True)

    successes = [r for r in results if isinstance(r, bytes)]
    failures = [r for r in results if isinstance(r, Exception)]
    # The first leader failed; the follower took over and delivered the file.
    assert len(successes) == 1, f"expected 1 success, got {len(successes)}: {results}"
    assert successes[0] == payload
    assert len(failures) == 1
    # Upstream calls: the failed leader's block-0 attempt (1) + the successful
    # follower downloading every block (num_blocks). No block downloaded twice
    # by the successful path.
    assert client.calls == num_blocks + 1, (
        f"expected {num_blocks + 1} upstream calls, got {client.calls}"
    )


# ---------------------------------------------------------------------------
# 4. resize under the exclusive meta lock
# ---------------------------------------------------------------------------

def test_concurrent_resize_never_tears_header(tmp_path):
    cache_dir = tmp_path / "cache"
    c0 = OlahCache(str(cache_dir), file_size=64, block_size=64, chunk_size=64)
    c0.close()

    sizes = [128, 192, 256, 320, 96]

    def worker(size):
        c = OlahCache(str(cache_dir), expected_etag=None)
        try:
            c.resize(size)
        finally:
            c.close()

    threads = [threading.Thread(target=worker, args=(s,)) for s in sizes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The final file_size must be one of the requested sizes (last writer wins),
    # never a torn value, and chunks.crc must match that size exactly.
    c = OlahCache(str(cache_dir), expected_etag=None)
    try:
        fs = c.header.file_size
        assert fs in sizes, f"torn file_size {fs} not in {sizes}"
        chunk_number = (fs + c.header.chunk_size - 1) // c.header.chunk_size
        crc_path = os.path.join(str(cache_dir), "chunks.crc")
        assert os.path.getsize(crc_path) == chunk_number * 4, (
            f"chunks.crc size {os.path.getsize(crc_path)} != {chunk_number * 4}"
        )
    finally:
        c.close()
