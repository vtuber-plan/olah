"""File blocks reuse the signed redirect target instead of re-resolving per block."""
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.configs import OlahConfig
from olah.server_routes import router

HTTP_CLIENT = httpx.AsyncClient
CONTENT = b"0123456789abcdefghijklmn"
FILE = "/team/tiny/resolve/main/tiny.bin"


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
    state = SimpleNamespace(resolves=0, signed_gets=[], expire_after_uses=None, canonical_gets=[], same_host_redirect=False)

    async def upstream(request):
        path = request.url.path
        if request.url.host == "cas.invalid":
            signature = request.url.params["sig"]
            state.signed_gets.append((signature, request.headers.get("authorization")))
            uses = sum(1 for sig, _ in state.signed_gets if sig == signature)
            if state.expire_after_uses is not None and uses > state.expire_after_uses:
                return httpx.Response(403, text="<Error>Request has expired</Error>")
            start, end = (int(x) for x in request.headers["range"].removeprefix("bytes=").split("-"))
            data = CONTENT[start : end + 1]
            return httpx.Response(
                206,
                headers={"content-length": str(len(data)), "content-range": f"bytes {start}-{end}/{len(CONTENT)}"},
                stream=_Bytes(data),
            )
        if "/paths-info/" in path:
            return httpx.Response(200, json=[{"type": "file", "path": "tiny.bin", "size": len(CONTENT), "oid": "2" * 40}])
        if path.startswith("/team/canonical/resolve/") and request.method == "GET":
            state.canonical_gets.append(request.headers.get("authorization"))
            start, end = (int(x) for x in request.headers["range"].removeprefix("bytes=").split("-"))
            data = CONTENT[start : end + 1]
            return httpx.Response(
                206,
                headers={"content-length": str(len(data)), "content-range": f"bytes {start}-{end}/{len(CONTENT)}"},
                stream=_Bytes(data),
            )
        if "/resolve/" in path and request.method == "GET" and state.same_host_redirect:
            state.resolves += 1
            return httpx.Response(302, headers={"location": "/team/canonical/resolve/main/tiny.bin"})
        if "/resolve/" in path and request.method == "GET":
            state.resolves += 1
            return httpx.Response(302, headers={"location": f"https://cas.invalid/object?sig={state.resolves}"})
        if request.method == "HEAD":
            return httpx.Response(200, headers={"etag": '"file-etag"'})
        return httpx.Response(200, json={"sha": "1" * 40})

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)

    async def request():
        async with HTTP_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
            return await client.get(FILE, headers={"authorization": "Bearer secret"})

    state.request = request
    return state


@pytest.mark.asyncio
async def test_blocks_reuse_the_signed_redirect_target(env):
    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert env.resolves == 1
    assert env.signed_gets == [("1", None)] * 3


@pytest.mark.asyncio
async def test_expired_redirect_target_is_resolved_again(env):
    env.expire_after_uses = 2

    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert env.resolves == 2
    assert [sig for sig, _ in env.signed_gets] == ["1", "1", "1", "2"]


@pytest.mark.asyncio
async def test_same_host_redirect_keeps_authorization(env):
    env.same_host_redirect = True

    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert env.resolves == 1
    assert env.canonical_gets == ["Bearer secret"] * 3
