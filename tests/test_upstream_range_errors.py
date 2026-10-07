"""How upstream failures of file range requests reach the client."""
import asyncio
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah import server_file_routes
from olah.configs import OlahConfig
from olah.server import app as olah_app
from olah.server_routes import router

HTTP_CLIENT = httpx.AsyncClient
CONTENT = b"0123456789abcdef"
FILE = "/team/tiny/resolve/main/tiny.bin"
SHA = "1" * 40


class _Bytes(httpx.AsyncByteStream):
    def __init__(self, data):
        self.data = data

    async def __aiter__(self):
        yield self.data


@pytest.fixture
def env(tmp_path, monkeypatch):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.hf_netloc = "upstream.invalid"
    config.cache_block_size = 8
    config.cache_chunk_size = 4
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(router)
    app.exception_handlers.update(olah_app.exception_handlers)
    range_responses = {}
    range_calls = []

    async def upstream(request):
        path = request.url.path
        if "/paths-info/" in path:
            payload = [{"type": "file", "path": "tiny.bin", "size": len(CONTENT), "oid": "2" * 40}]
            return httpx.Response(200, json=payload)
        if "/resolve/" in path and request.method == "GET":
            start, end = (int(x) for x in request.headers["range"].removeprefix("bytes=").split("-"))
            range_calls.append(start)
            queued = range_responses.get(start)
            if queued:
                item = queued.pop(0)
                return await item() if callable(item) else item
            data = CONTENT[start : end + 1]
            return httpx.Response(
                206,
                headers={"content-length": str(len(data)), "content-range": f"bytes {start}-{end}/{len(CONTENT)}"},
                stream=_Bytes(data),
            )
        if request.method == "HEAD":
            return httpx.Response(200, headers={"etag": '"file-etag"'})
        return httpx.Response(200, json={"sha": SHA})

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)

    async def request(method="GET", headers=None):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with HTTP_CLIENT(transport=transport, base_url="http://olah.test") as client:
            return await client.request(method, FILE, headers=headers)

    return SimpleNamespace(request=request, range_responses=range_responses, range_calls=range_calls)


def _rate_limited(retry_after):
    return httpx.Response(429, headers={"retry-after": str(retry_after), "ratelimit": f'"resolvers";r=0;t={retry_after}'})


@pytest.mark.asyncio
async def test_rate_limit_on_first_range_is_relayed_as_status(env):
    env.range_responses[0] = [_rate_limited(60)]

    response = await env.request()

    assert response.status_code == 429
    assert response.headers["retry-after"] == "60"
    assert response.headers["ratelimit"] == '"resolvers";r=0;t=60'
    assert env.range_calls == [0]


@pytest.mark.asyncio
async def test_client_error_on_first_range_is_relayed_as_status(env):
    env.range_responses[0] = [httpx.Response(403, text="forbidden")]

    response = await env.request()

    assert response.status_code == 403
    assert b"forbidden" not in response.content


@pytest.mark.asyncio
@pytest.mark.parametrize("first_block_failure", [httpx.Response(503, text="unavailable"), _rate_limited(60)])
async def test_mid_stream_failure_aborts_and_resume_gets_the_status(env, first_block_failure):
    env.range_responses[8] = [first_block_failure, _rate_limited(60)]

    response = await env.request()

    assert response.status_code == 200
    assert int(response.headers["content-length"]) == len(CONTENT)
    assert response.content == CONTENT[:8]

    resumed = await env.request(headers={"range": "bytes=8-"})
    assert resumed.status_code == 429
    assert resumed.headers["retry-after"] == "60"


@pytest.mark.asyncio
async def test_server_error_on_first_range_still_aborts_for_client_resume(env):
    env.range_responses[0] = [httpx.Response(503, text="unavailable")]

    response = await env.request()

    assert response.status_code == 200
    assert response.content == b""
    assert env.range_calls == [0]


def _shorten_header_hold(monkeypatch):
    # test_proxy_files re-imports olah.proxy.files; patch the copy the routes use.
    files_globals = server_file_routes.file_get_generator.__globals__
    monkeypatch.setitem(files_globals, "HEADER_HOLD_TIMEOUT", 0.05)


def _slow(response):
    async def respond():
        await asyncio.sleep(0.2)
        return response

    return respond


@pytest.mark.asyncio
async def test_slow_first_range_commits_headers_and_keeps_streaming(env, monkeypatch):
    _shorten_header_hold(monkeypatch)
    data = CONTENT[:8]
    env.range_responses[0] = [
        _slow(httpx.Response(206, headers={"content-length": "8", "content-range": "bytes 0-7/16"}, stream=_Bytes(data)))
    ]

    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert env.range_calls == [0, 8]


@pytest.mark.asyncio
async def test_refusal_after_header_hold_aborts_instead(env, monkeypatch):
    _shorten_header_hold(monkeypatch)
    env.range_responses[0] = [_slow(_rate_limited(60))]

    response = await env.request()

    assert response.status_code == 200
    assert response.content == b""
