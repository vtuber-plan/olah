import asyncio
import gc

import httpx
import pytest

from olah.proxy import api_proxy
from olah.utils.cache_utils import read_cache_request

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
