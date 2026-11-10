import asyncio
import gc
import gzip

import httpx
import pytest

from olah.proxy import api_proxy
from olah.utils.cache_utils import read_cache_request, write_cache_request

HTTP_CLIENT = httpx.AsyncClient


class GatedStream(httpx.AsyncByteStream):
    """Yields the first chunk, then waits for ``release`` before the second."""

    def __init__(self, release: asyncio.Event):
        self.release = release
        self.closed = False

    async def __aiter__(self):
        yield b'{"a":'
        await self.release.wait()
        yield b"1}"

    async def aclose(self):
        self.closed = True


class OneShotStream(httpx.AsyncByteStream):
    """Yields a fixed list of raw (still content-encoded) chunks."""

    def __init__(self, chunks):
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self):
        pass


@pytest.fixture
def gzip_upstream(monkeypatch):
    payload = b'{"files": [{"rfilename": "model.gguf", "size": 42}]}'
    encoded = gzip.compress(payload)
    state = {"calls": 0, "payload": payload}

    async def handler(request):
        state["calls"] += 1
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "content-encoding": "gzip",
                "content-length": str(len(encoded)),
                "transfer-encoding": "chunked",
                "set-cookie": "session=abc; Path=/",
            },
            stream=OneShotStream([encoded[:16], encoded[16:]]),
        )

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(handler))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(api_proxy.httpx, "AsyncClient", UpstreamClient)
    return state


@pytest.fixture
def failing_upstream(monkeypatch):
    state = {"calls": 0}

    async def handler(request):
        state["calls"] += 1
        return httpx.Response(
            429, headers={"content-type": "text/plain"}, stream=OneShotStream([b"rate limited"])
        )

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(handler))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(api_proxy.httpx, "AsyncClient", UpstreamClient)
    return state


@pytest.fixture
def upstream(monkeypatch):
    state = {"calls": 0, "streams": []}

    async def handler(request):
        state["calls"] += 1
        stream = GatedStream(state["release"])
        state["streams"].append(stream)
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=stream)

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(handler))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(api_proxy.httpx, "AsyncClient", UpstreamClient)
    return state


@pytest.mark.asyncio
async def test_streams_from_a_single_request_and_caches_on_completion(upstream, tmp_path):
    upstream["release"] = asyncio.Event()
    save_path = str(tmp_path / "meta_get.json")

    result = await api_proxy.proxy_api_request("https://hub.invalid/api/x", "GET", {}, True, save_path)

    assert result.status_code == 200
    assert await result.body.__anext__() == b'{"a":'
    upstream["release"].set()
    assert [chunk async for chunk in result.body] == [b"1}"]
    assert upstream["calls"] == 1
    assert (await read_cache_request(save_path))["content"] == b'{"a":1}'


@pytest.mark.asyncio
async def test_unconsumed_body_releases_the_upstream_stream(upstream, tmp_path):
    upstream["release"] = asyncio.Event()

    result = await api_proxy.proxy_api_request(
        "https://hub.invalid/api/x", "GET", {}, True, str(tmp_path / "meta_get.json")
    )
    del result
    gc.collect()
    # The finalizer schedules aclose() on the loop; let it run.
    for _ in range(10):
        await asyncio.sleep(0)

    assert upstream["streams"][0].closed
    assert not (tmp_path / "meta_get.json").exists()


@pytest.mark.asyncio
async def test_gzip_upstream_is_relayed_identity_encoded(gzip_upstream, tmp_path):
    save_path = str(tmp_path / "tree_get.json")

    result = await api_proxy.proxy_api_request("https://hub.invalid/api/x", "GET", {}, True, save_path)

    assert result.status_code == 200
    relayed = {name.lower() for name in result.headers}
    assert "content-encoding" not in relayed
    assert "content-length" not in relayed
    assert "transfer-encoding" not in relayed
    assert "set-cookie" not in relayed
    assert gzip_upstream["calls"] == 1

    chunks = [chunk async for chunk in result.body]
    assert b"".join(chunks) == gzip_upstream["payload"]

    cached = await read_cache_request(save_path)
    assert cached["content"] == gzip_upstream["payload"]
    cached_headers = {key.lower(): value for key, value in cached["headers"].items()}
    assert "content-encoding" not in cached_headers
    assert cached_headers["content-length"] == str(len(gzip_upstream["payload"]))


@pytest.mark.asyncio
async def test_legacy_cached_gzip_body_is_decoded_on_replay(tmp_path):
    from olah.proxy.tree import _tree_cache_generator

    payload = b'[{"rfilename": "model.gguf"}]'
    encoded = gzip.compress(payload)
    save_path = str(tmp_path / "tree_get.json")
    await write_cache_request(
        save_path,
        200,
        {
            "Content-Encoding": "gzip",
            "Content-Length": str(len(encoded)),
            "Content-Type": "application/json",
        },
        encoded,
    )

    result = await _tree_cache_generator(save_path)
    assert result.status_code == 200
    headers = {key.lower(): value for key, value in result.headers.items()}
    assert "content-encoding" not in headers
    assert headers["content-length"] == str(len(payload))
    assert headers["content-type"] == "application/json"
    assert [chunk async for chunk in result.body] == [payload]


@pytest.mark.asyncio
async def test_stale_copy_decodes_legacy_cached_body(failing_upstream, tmp_path):
    payload = b'{"refs": []}'
    save_path = str(tmp_path / "meta_get.json")
    await write_cache_request(save_path, 200, {"content-encoding": "gzip"}, gzip.compress(payload))

    result = await api_proxy.proxy_api_request(
        "https://hub.invalid/api/x", "GET", {}, False, save_path, max_stale=3600
    )
    assert result.status_code == 200
    assert failing_upstream["calls"] == 1
    headers = {key.lower(): value for key, value in result.headers.items()}
    assert "content-encoding" not in headers
    assert headers["content-length"] == str(len(payload))
    assert [chunk async for chunk in result.body] == [payload]


def test_decode_cached_response_encodings():
    import brotli
    import zlib

    from olah.utils.encoding_utils import decode_cached_response

    payload = b'{"ok": true}'

    body, headers = decode_cached_response(brotli.compress(payload), {"Content-Encoding": "br"})
    assert body == payload
    assert headers["content-length"] == str(len(payload))
    assert "content-encoding" not in headers

    body, headers = decode_cached_response(zlib.compress(payload), {"content-encoding": "deflate"})
    assert body == payload

    body, headers = decode_cached_response(payload, {"content-type": "application/json"})
    assert body == payload
    assert headers["content-length"] == str(len(payload))

    # An unsupported encoding is relayed verbatim, as before.
    body, headers = decode_cached_response(b"\x00\x01", {"content-encoding": "zstd"})
    assert body == b"\x00\x01"
    assert headers["content-encoding"] == "zstd"
