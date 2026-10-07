"""A cached repo visibility result only ever answers for the caller it was confirmed for."""
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.configs import OlahConfig
from olah.server_routes import router

HTTP_CLIENT = httpx.AsyncClient
SHA = "1" * 40
TREE = f"/api/models/team/private/tree/{SHA}"
REVISION = "/api/models/team/private/revision/main"


class _Bytes(httpx.AsyncByteStream):
    def __init__(self, data):
        self.data = data

    async def __aiter__(self):
        yield self.data


def _json(payload):
    return httpx.Response(200, headers={"content-type": "application/json"}, stream=_Bytes(json.dumps(payload).encode()))


@pytest.fixture
def env(tmp_path, monkeypatch):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.hf_netloc = "upstream.invalid"
    assert config.metadata_cache_ttl > 0
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(router)
    calls = []

    async def upstream(request):
        calls.append((request.method, request.url.path, request.headers.get("authorization")))
        # A private repo: only alice's token can see it.
        if request.headers.get("authorization") != "Bearer alice":
            return httpx.Response(401, headers={"x-error-code": "RepoNotFound"})
        if "/tree/" in request.url.path:
            return _json([{"type": "file", "path": "secret.bin", "size": 3, "oid": "2" * 40}])
        return _json({"id": "team/private", "sha": SHA, "private": True})

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)

    async def request(path, token=None):
        headers = {"authorization": token} if token else {}
        async with HTTP_CLIENT(
            transport=httpx.ASGITransport(app=app), base_url="http://olah.test", follow_redirects=True
        ) as client:
            return await client.get(path, headers=headers)

    return SimpleNamespace(request=request, calls=calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [TREE, REVISION])
@pytest.mark.parametrize("other", [None, "Bearer mallory"])
async def test_one_callers_access_does_not_open_the_repo_to_others(env, path, other):
    assert (await env.request(path, "Bearer alice")).status_code == 200

    response = await env.request(path, other)

    assert response.status_code in (401, 404)
    assert b"secret.bin" not in response.content
    assert SHA.encode() not in response.content


@pytest.mark.asyncio
async def test_a_callers_own_access_is_reused_within_the_ttl(env):
    assert (await env.request(TREE, "Bearer alice")).status_code == 200
    env.calls.clear()

    assert (await env.request(TREE, "Bearer alice")).status_code == 200
    assert env.calls == []
