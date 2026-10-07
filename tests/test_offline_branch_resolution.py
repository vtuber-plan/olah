"""Branches downloaded online through the file routes stay resolvable offline."""
import json
import os
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.configs import OlahConfig, OlahRuleList
from olah.server_routes import router
from olah.utils.cache_utils import write_cache_request
from olah.utils.repo_utils import get_meta_save_path, get_resolved_commit_save_path

HTTP_CLIENT = httpx.AsyncClient
OLD_SHA = "1" * 40
NEW_SHA = "2" * 40
CONTENT = b"tiny file content"
FILE = "/team/demo/resolve/main/file.bin"


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
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(router)
    state = SimpleNamespace(config=config, sha=NEW_SHA)

    async def upstream(request):
        path = request.url.path
        if "/paths-info/" in path:
            payload = [{"type": "file", "path": "file.bin", "size": len(CONTENT), "oid": "3" * 40}]
            return httpx.Response(200, stream=_Bytes(json.dumps(payload).encode()))
        if "/resolve/" in path and request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"etag": '"blob"', "content-length": str(len(CONTENT)), "x-repo-commit": state.sha},
                stream=_Bytes(b""),
            )
        if "/resolve/" in path:
            start, end = (int(x) for x in request.headers["range"].removeprefix("bytes=").split("-"))
            data = CONTENT[start : end + 1]
            return httpx.Response(
                206,
                headers={"content-length": str(len(data)), "content-range": f"bytes {start}-{end}/{len(CONTENT)}"},
                stream=_Bytes(data),
            )
        raise AssertionError(f"unexpected upstream call {request.method} {path}")

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)

    async def request(path=FILE):
        async with HTTP_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
            return await client.get(path)

    state.request = request
    return state


@pytest.mark.asyncio
async def test_branch_downloaded_online_resolves_offline(env):
    assert (await env.request()).content == CONTENT
    env.config.offline = True

    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert response.headers["x-repo-commit"] == NEW_SHA


@pytest.mark.asyncio
async def test_most_recent_branch_mapping_wins(env):
    repos_path = env.config.repos_path
    stale_meta = get_meta_save_path(repos_path, "models", "team", "demo", "main")
    os.makedirs(os.path.dirname(stale_meta))
    await write_cache_request(stale_meta, 200, {}, json.dumps({"sha": OLD_SHA}).encode())
    with open(stale_meta, "r", encoding="utf-8") as f:
        envelope = json.load(f)
    envelope["cached_at"] -= 60
    with open(stale_meta, "w", encoding="utf-8") as f:
        json.dump(envelope, f)

    assert (await env.request()).content == CONTENT
    env.config.offline = True

    assert (await env.request()).headers["x-repo-commit"] == NEW_SHA


@pytest.mark.asyncio
async def test_mapping_respects_cache_rules(env):
    env.config.cache = OlahRuleList.from_list([{"repo": "*", "allow": False}])

    assert (await env.request()).status_code == 200

    path = get_resolved_commit_save_path(env.config.repos_path, "models", "team", "demo", "main")
    assert not os.path.exists(path)
