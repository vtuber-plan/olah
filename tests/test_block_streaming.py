# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""A cache-miss block must reach the client while it is still downloading.

The block cache fetches one whole block (64 MiB by default) per upstream request.
Clients time out on idle sockets rather than slow ones -- huggingface_hub's
``HF_HUB_DOWNLOAD_TIMEOUT`` defaults to 10s -- so if the proxy forwards a block
only once it is complete, any block slower than that fails every attempt: each
disconnect cancels the download before it is published, caching no progress.
"""

import asyncio
import os
import random
import threading
from types import SimpleNamespace

import brotli
import pytest

pytest.importorskip("portalocker")

from olah.cache.olah_cache import CacheIntegrityError, OlahCache  # noqa: E402
from olah.proxy import files as proxy_files  # noqa: E402


def _make_app(tmp_path, block_size, chunk_size):
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


class _GatedStreamResponse:
    """Streams a range body in ``parts`` pieces, parking on ``gate`` after
    ``gate_after`` of them.

    While the gate is closed the download cannot finish, so "has the proxy
    forwarded anything yet?" is decidable rather than a race. ``gate_after`` must
    exceed 1, because the downloader withholds its most recent piece until the
    block is published.
    """

    def __init__(self, body: bytes, gate: asyncio.Event, parts: int, gate_after: int):
        self.status_code = 206
        self.headers = {"content-length": str(len(body))}
        self._body = body
        self._gate = gate
        self._parts = parts
        self._gate_after = gate_after

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def aiter_raw(self):
        step = max(1, len(self._body) // self._parts)
        for i, offset in enumerate(range(0, len(self._body), step)):
            if i >= self._gate_after:
                await self._gate.wait()
            yield self._body[offset : offset + step]


class _GatedRangeClient:
    """Range-aware fake httpx client whose bodies arrive in gated pieces."""

    def __init__(
        self, payload: bytes, gate: asyncio.Event, parts: int = 8, gate_after: int = 2
    ):
        self.payload = payload
        self.calls = 0
        self._gate = gate
        self._parts = parts
        self._gate_after = gate_after
        self._lock = threading.Lock()

    def stream(self, **kwargs):
        headers = kwargs.get("headers") or {}
        rng = headers.get("range", "")
        if rng.startswith("bytes="):
            start, _, end = rng[6:].partition("-")
            body = self.payload[int(start) : int(end) + 1 if end else len(self.payload)]
        else:
            body = self.payload
        with self._lock:
            self.calls += 1
        return _GatedStreamResponse(body, self._gate, self._parts, self._gate_after)


async def _collect(gen):
    return b"".join([chunk async for chunk in gen])


async def _collect_no_drain(gen, expected_len):
    """Consume exactly ``expected_len`` bytes then stop, never pulling again.

    This is what a server does once content-length is satisfied, and it is the
    one consumer shape that a drain-to-exhaustion test cannot see.
    """
    out = b""
    async for chunk in gen:
        out += chunk
        if len(out) >= expected_len:
            break
    await gen.aclose()
    return out


def _assert_fully_cached(save_path):
    cache = OlahCache(str(save_path))
    try:
        missing = [
            i for i in range(cache._get_block_number()) if not cache.has_block(i)
        ]
        assert not missing, f"blocks left unpublished: {missing}"
    finally:
        cache.close()


async def _assert_cached_blocks_match(save_path, payload):
    """Every published block must hold exactly the payload bytes it covers."""
    if not os.path.exists(save_path):
        return
    cache = OlahCache(str(save_path))
    try:
        bs = cache._get_block_size()
        for idx in range(cache._get_block_number()):
            if not cache.has_block(idx):
                continue
            start = idx * bs
            end = min(start + bs, len(payload))
            padded = await cache.read_block(idx)
            assert padded is not None
            assert padded[: end - start] == payload[start:end], f"block {idx} corrupt"
    finally:
        cache.close()


class _PlainStreamResponse:
    """Ungated range body, delivered in ``parts`` pieces.

    ``content_length`` can be set independently of the body to forge the
    truncated-but-plausible response that the byte-count check cannot catch.
    """

    def __init__(self, body: bytes, parts: int, content_length: int):
        self.status_code = 206
        self.headers = {"content-length": str(content_length)}
        self._body = body
        self._parts = parts

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def aiter_raw(self):
        if not self._body:
            return
        step = max(1, len(self._body) // self._parts)
        for offset in range(0, len(self._body), step):
            yield self._body[offset : offset + step]


class _RangeClient:
    """Range-aware fake httpx client. ``truncate`` drops trailing body bytes
    while still advertising the full length."""

    def __init__(self, payload: bytes, parts: int = 3, truncate: int = 0):
        self.payload = payload
        self.calls = 0
        self._parts = parts
        self._truncate = truncate
        self._lock = threading.Lock()

    def stream(self, **kwargs):
        headers = kwargs.get("headers") or {}
        rng = headers.get("range", "")
        if rng.startswith("bytes="):
            start, _, end = rng[6:].partition("-")
            body = self.payload[int(start) : int(end) + 1 if end else len(self.payload)]
        else:
            body = self.payload
        with self._lock:
            self.calls += 1
        advertised = len(body)
        if self._truncate:
            body = body[: max(0, len(body) - self._truncate)]
        return _PlainStreamResponse(body, self._parts, advertised)


def _fetch(app, save_path, client, payload, headers=None):
    return proxy_files._file_chunk_get(
        app=app,
        save_path=str(save_path),
        client=client,
        method="GET",
        url="https://huggingface.co/team/demo/resolve/main/blob.bin",
        headers=headers or {},
        allow_cache=True,
        file_size=len(payload),
    )


@pytest.mark.asyncio
async def test_block_reaches_client_before_download_completes(tmp_path):
    block_size = 64
    payload = bytes((i * 7) % 256 for i in range(block_size))  # exactly one block
    save_path = tmp_path / "cache"
    gate = asyncio.Event()
    client = _GatedRangeClient(payload, gate, parts=8)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    gen = _fetch(app, save_path, client, payload)

    # The gate is still closed, so the block cannot have finished downloading.
    # A prefix must nonetheless be available to the client.
    first = await asyncio.wait_for(gen.__anext__(), timeout=5)
    assert first, "proxy forwarded nothing before the block finished downloading"
    assert len(first) < len(
        payload
    ), f"proxy buffered the whole {len(payload)}-byte block before forwarding"
    assert payload.startswith(first)

    # Release the rest: the response must still be byte-exact...
    gate.set()
    rest = b"".join([chunk async for chunk in gen])
    assert first + rest == payload

    # ...and the completed block must still be published to the cache.
    cache = OlahCache(str(save_path))
    try:
        assert cache.is_fully_cached()
    finally:
        cache.close()

    # A second request is served entirely from cache -- no new upstream call.
    calls_before = client.calls
    assert await _collect(_fetch(app, save_path, client, payload)) == payload
    assert client.calls == calls_before


@pytest.mark.asyncio
async def test_partial_block_range_streams_and_caches_whole_block(tmp_path):
    """A client range covering part of a block still caches the *whole* block.

    Guards the window arithmetic: incremental emission must clip to the client's
    requested range while the accumulated buffer stays the full block.
    """
    block_size = 64
    payload = bytes((i * 11) % 256 for i in range(block_size))
    save_path = tmp_path / "cache"
    gate = asyncio.Event()
    gate.set()  # no pausing needed here; this test is about correctness of the window
    client = _GatedRangeClient(payload, gate, parts=8)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    got = await _collect(
        _fetch(app, save_path, client, payload, headers={"range": "bytes=10-29"})
    )
    assert got == payload[10:30]

    cache = OlahCache(str(save_path))
    try:
        assert (
            cache.is_fully_cached()
        ), "whole block should be cached, not just the window"
    finally:
        cache.close()


@pytest.mark.asyncio
async def test_multi_block_response_streams_each_block_incrementally(tmp_path):
    """Every block streams; the client never waits a full block for its first byte."""
    block_size = 64
    payload = bytes((i * 3) % 256 for i in range(block_size * 2 + 20))  # 3 blocks
    save_path = tmp_path / "cache"
    gate = asyncio.Event()
    client = _GatedRangeClient(payload, gate, parts=8)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    gen = _fetch(app, save_path, client, payload)
    first = await asyncio.wait_for(gen.__anext__(), timeout=5)
    assert 0 < len(first) < block_size

    gate.set()
    rest = b"".join([chunk async for chunk in gen])
    assert first + rest == payload

    cache = OlahCache(str(save_path))
    try:
        assert cache.is_fully_cached()
    finally:
        cache.close()


# ---------------------------------------------------------------------------
# differential fuzz: every range shape, block/chunk geometry and cache state
# ---------------------------------------------------------------------------


def _random_range(rnd, file_size):
    """A range header plus the bytes it should resolve to."""
    kind = rnd.choice(["full", "mid", "prefix", "open_ended", "single_byte"])
    if kind == "full":
        return None, bytes()  # header None means whole file
    if kind == "single_byte":
        pos = rnd.randrange(file_size)
        return f"bytes={pos}-{pos}", (pos, pos + 1)
    start = rnd.randrange(file_size)
    if kind == "open_ended":
        return f"bytes={start}-", (start, file_size)
    if kind == "prefix":
        end = rnd.randrange(0, file_size)
        return f"bytes=0-{end}", (0, end + 1)
    end = rnd.randrange(start, file_size)
    return f"bytes={start}-{end}", (start, end + 1)


@pytest.mark.parametrize("seed", range(80))
@pytest.mark.asyncio
async def test_fuzz_response_and_cache_are_byte_exact(tmp_path, seed):
    rnd = random.Random(seed)
    chunk_size = rnd.choice([1, 2, 4, 8, 16])
    block_size = chunk_size * rnd.choice([1, 2, 3, 5])
    file_size = rnd.randint(0, 400)
    payload = bytes(rnd.randrange(256) for _ in range(file_size))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=chunk_size)
    client = _RangeClient(payload, parts=rnd.choice([1, 2, 3, 7]))

    # Sometimes warm an arbitrary sub-range first, so the request under test
    # stitches cached and uncached blocks together.
    if file_size and rnd.random() < 0.6:
        a = rnd.randrange(file_size)
        b = rnd.randrange(a, file_size)
        await _collect(
            _fetch(app, save_path, client, payload, headers={"range": f"bytes={a}-{b}"})
        )
        await _assert_cached_blocks_match(save_path, payload)

    if file_size == 0:
        header, expected = None, b""
    else:
        header, span = _random_range(rnd, file_size)
        expected = payload if header is None else payload[span[0] : span[1]]

    headers = {} if header is None else {"range": header}
    got = await _collect(_fetch(app, save_path, client, payload, headers=headers))
    assert got == expected, f"seed={seed} bs={block_size} cs={chunk_size} r={header}"
    await _assert_cached_blocks_match(save_path, payload)
    if header is None and file_size:
        _assert_fully_cached(save_path)

    # Re-reading the same range is byte-identical and needs no new upstream call.
    calls_before = client.calls
    again = await _collect(_fetch(app, save_path, client, payload, headers=headers))
    assert again == expected
    assert client.calls == calls_before, "warm re-read refetched from upstream"


@pytest.mark.parametrize("compression", ["none", "gzip", "lzma"])
@pytest.mark.asyncio
async def test_streaming_round_trips_under_every_cache_compression(
    tmp_path, compression
):
    block_size, chunk_size = 32, 8
    payload = bytes((i * 37) % 256 for i in range(block_size * 3 + 5))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=chunk_size)
    app.state.app_settings.config.cache_compression = compression
    client = _RangeClient(payload, parts=3)

    assert await _collect(_fetch(app, save_path, client, payload)) == payload
    await _assert_cached_blocks_match(save_path, payload)
    _assert_fully_cached(save_path)
    # And warm, from the compressed blocks, including a mid-block window.
    calls_before = client.calls
    got = await _collect(
        _fetch(app, save_path, client, payload, headers={"range": "bytes=13-77"})
    )
    assert got == payload[13:78]
    assert client.calls == calls_before


# ---------------------------------------------------------------------------
# malformed upstream and abandoned clients
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_truncated_body_with_intact_content_length_is_rejected(tmp_path):
    """A short body that still advertises the full length must not be cached.

    ``_get_file_range_from_remote`` believes the header over the bytes it saw, so
    this is caught by the block's own completeness check.
    """
    block_size = 64
    payload = bytes((i * 19) % 256 for i in range(block_size))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)
    client = _RangeClient(payload, parts=4, truncate=10)

    with pytest.raises(Exception, match="incomplete"):
        await _collect(_fetch(app, save_path, client, payload))

    cache = OlahCache(str(save_path))
    try:
        assert not cache.has_block(0), "short block must not be published"
    finally:
        cache.close()

    # An honest upstream then repairs it.
    good = _RangeClient(payload, parts=4)
    assert await _collect(_fetch(app, save_path, good, payload)) == payload
    await _assert_cached_blocks_match(save_path, payload)


@pytest.mark.asyncio
async def test_client_abandoning_mid_block_publishes_nothing_and_releases_lock(
    tmp_path,
):
    block_size = 64
    payload = bytes((i * 17) % 256 for i in range(block_size * 2))
    save_path = tmp_path / "cache"
    gate = asyncio.Event()
    client = _GatedRangeClient(payload, gate, parts=8)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    gen = _fetch(app, save_path, client, payload)
    assert await asyncio.wait_for(gen.__anext__(), timeout=5)
    await gen.aclose()  # client disconnects mid-block

    cache = OlahCache(str(save_path))
    try:
        assert not cache.has_block(0), "partial block must not be published"
    finally:
        cache.close()

    # The per-block download lock was released, so a fresh request completes.
    gate.set()
    fresh = _GatedRangeClient(payload, gate, parts=8)
    assert await _collect(_fetch(app, save_path, fresh, payload)) == payload
    await _assert_cached_blocks_match(save_path, payload)


@pytest.mark.asyncio
async def test_cache_integrity_failure_resumes_mid_block(tmp_path, monkeypatch):
    """A cache-hit that rots mid-stream resumes at a non-zero block offset."""
    block_size = 64
    payload = bytes((i * 29) % 256 for i in range(block_size * 2 + 13))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)
    client = _RangeClient(payload, parts=3)
    assert await _collect(_fetch(app, save_path, client, payload)) == payload

    real_stream_range = OlahCache.stream_range
    calls = {"n": 0}

    async def flaky(self, start_pos, end_pos):
        calls["n"] += 1
        if calls["n"] == 1:
            yield payload[start_pos : start_pos + 7]
            raise CacheIntegrityError("simulated bit-rot")
        async for piece in real_stream_range(self, start_pos, end_pos):
            yield piece

    monkeypatch.setattr(OlahCache, "stream_range", flaky)
    assert await _collect(_fetch(app, save_path, client, payload)) == payload


# ---------------------------------------------------------------------------
# single-flight still holds while streaming
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_streaming_requests_share_one_download(tmp_path):
    block_size = 64
    payload = bytes((i * 23) % 256 for i in range(block_size * 2 + 11))
    num_blocks = (len(payload) + block_size - 1) // block_size
    save_path = tmp_path / "cache"
    gate = asyncio.Event()
    gate.set()
    client = _GatedRangeClient(payload, gate, parts=8)
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)

    results = await asyncio.gather(
        *(_collect(_fetch(app, save_path, client, payload)) for _ in range(5))
    )
    assert all(r == payload for r in results)
    assert (
        client.calls == num_blocks
    ), f"expected {num_blocks} upstream downloads, got {client.calls}"
    await _assert_cached_blocks_match(save_path, payload)


@pytest.mark.asyncio
async def test_final_block_is_published_when_consumer_stops_at_content_length(tmp_path):
    """The last block of a response must be cached even if nobody pulls again.

    Publication used to sit after the generator's final ``yield``, so it only ran
    for a consumer that drained to ``StopAsyncIteration``. A server that stops at
    content-length does not, which left every response's last block uncached and
    re-fetched from upstream on every request.
    """
    block_size = 64
    payload = bytes((i * 41) % 256 for i in range(block_size * 2 + 21))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)
    client = _RangeClient(payload, parts=4)

    got = await _collect_no_drain(_fetch(app, save_path, client, payload), len(payload))
    assert got == payload
    _assert_fully_cached(save_path)
    await _assert_cached_blocks_match(save_path, payload)

    calls_before = client.calls
    assert await _collect(_fetch(app, save_path, client, payload)) == payload
    assert client.calls == calls_before, "warm re-read refetched the final block"


@pytest.mark.parametrize("seed", range(40))
@pytest.mark.asyncio
async def test_fuzz_non_draining_consumer_still_fully_caches(tmp_path, seed):
    """Same as the range fuzz, but the consumer never pulls past its last byte."""
    rnd = random.Random(1000 + seed)
    chunk_size = rnd.choice([1, 2, 4, 8, 16])
    block_size = chunk_size * rnd.choice([1, 2, 3, 5])
    file_size = rnd.randint(1, 400)
    payload = bytes(rnd.randrange(256) for _ in range(file_size))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=chunk_size)
    client = _RangeClient(payload, parts=rnd.choice([1, 2, 3, 7]))

    got = await _collect_no_drain(_fetch(app, save_path, client, payload), file_size)
    assert got == payload, f"seed={seed} bs={block_size} cs={chunk_size}"
    _assert_fully_cached(save_path)
    await _assert_cached_blocks_match(save_path, payload)
    calls_before = client.calls
    assert await _collect(_fetch(app, save_path, client, payload)) == payload
    assert client.calls == calls_before


@pytest.mark.parametrize("drain", [True, False])
@pytest.mark.asyncio
async def test_uncached_mode_streams_exactly_and_stores_nothing(tmp_path, drain):
    """``allow_cache=False`` must still deliver every byte, publishing none of it."""
    block_size = 32
    payload = bytes((i * 13) % 256 for i in range(block_size * 3 + 7))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)
    client = _RangeClient(payload, parts=3)

    def fetch():
        return proxy_files._file_chunk_get(
            app=app,
            save_path=str(save_path),
            client=client,
            method="GET",
            url="https://huggingface.co/team/demo/resolve/main/blob.bin",
            headers={},
            allow_cache=False,
            file_size=len(payload),
        )

    got = (
        await _collect(fetch())
        if drain
        else await _collect_no_drain(fetch(), len(payload))
    )
    assert got == payload

    cache = OlahCache(str(save_path))
    try:
        assert not any(
            cache.has_block(i) for i in range(cache._get_block_number())
        ), "allow_cache=False must not publish blocks"
    finally:
        cache.close()

    # Every read goes upstream, since nothing was cached.
    calls_before = client.calls
    assert await _collect(fetch()) == payload
    assert client.calls > calls_before


class _BrotliRangeClient:
    """Serves each range brotli-encoded, as HF's CDN may for some objects.

    ``_get_file_range_from_remote`` decompresses before the block sees the bytes,
    so the block's own length check must compare against the *decompressed* size.
    """

    def __init__(self, payload: bytes, parts: int = 3):
        self.payload = payload
        self.calls = 0
        self._parts = parts

    def stream(self, **kwargs):
        headers = kwargs.get("headers") or {}
        rng = headers.get("range", "")
        start, _, end = rng[6:].partition("-")
        body = self.payload[int(start) : int(end) + 1]
        self.calls += 1
        encoded = brotli.compress(body)
        outer = self

        class _Resp:
            status_code = 206
            headers = {"content-encoding": "br"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def aiter_raw(self):
                step = max(1, len(encoded) // outer._parts)
                for off in range(0, len(encoded), step):
                    yield encoded[off : off + step]

        return _Resp()


@pytest.mark.asyncio
async def test_compressed_upstream_response_streams_and_caches(tmp_path):
    block_size = 32
    payload = bytes((i * 31) % 256 for i in range(block_size * 3 + 9))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)
    client = _BrotliRangeClient(payload)

    assert await _collect(_fetch(app, save_path, client, payload)) == payload
    _assert_fully_cached(save_path)
    await _assert_cached_blocks_match(save_path, payload)

    calls_before = client.calls
    assert await _collect(_fetch(app, save_path, client, payload)) == payload
    assert client.calls == calls_before


@pytest.mark.asyncio
async def test_multi_range_request_streams_each_span(tmp_path):
    """A multi-range request runs the window arithmetic once per span."""
    block_size = 16
    payload = bytes((i * 7) % 256 for i in range(block_size * 5))
    save_path = tmp_path / "cache"
    app = _make_app(tmp_path, block_size=block_size, chunk_size=block_size)
    client = _RangeClient(payload, parts=3)

    got = await _collect(
        _fetch(
            app,
            save_path,
            client,
            payload,
            headers={"range": "bytes=3-9, 40-55, 70-79"},
        )
    )
    assert got == payload[3:10] + payload[40:56] + payload[70:80]
    await _assert_cached_blocks_match(save_path, payload)

    # Warm, and with the spans overlapping a single block boundary.
    calls_before = client.calls
    again = await _collect(
        _fetch(
            app,
            save_path,
            client,
            payload,
            headers={"range": "bytes=3-9, 40-55, 70-79"},
        )
    )
    assert again == got
    assert client.calls == calls_before
