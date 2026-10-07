"""Serving cached content to recently verified callers while the Hub can't answer."""
import json
import os
import time
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.configs import OlahConfig
from olah.errors import UpstreamRateLimited
from olah.server import upstream_rate_limited_handler
from olah.server_routes import router
from olah.utils.upstream_fallback import UpstreamFallbackMiddleware

HTTP_CLIENT = httpx.AsyncClient
SHA = "1" * 40
CONTENT = b"tiny file content"
FILE = "/team/demo/resolve/main/file.bin"
RATE_LIMIT_HEADERS = {"retry-after": "60", "ratelimit": '"resolvers";r=0;t=60'}
# Hub failure mode -> status a client gets when the cache can't answer instead.
FAILURES = {"rate_limited": 429, "server_error": 504, "unreachable": 504}


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
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(router)
    app.add_exception_handler(UpstreamRateLimited, upstream_rate_limited_handler)
    app.add_middleware(UpstreamFallbackMiddleware)
    state = SimpleNamespace(config=config, failure=None, access_checks_succeed=False, calls=[])

    async def upstream(request):
        state.calls.append((request.method, request.url.path))
        path = request.url.path
        access_check = request.method == "HEAD" and (path == "/api/models/team/demo" or "/resolve/" in path)
        failing = state.failure is not None and not (access_check and state.access_checks_succeed)
        if failing and state.failure == "rate_limited":
            return httpx.Response(429, headers=RATE_LIMIT_HEADERS, stream=_Bytes(b""))
        if failing and state.failure == "server_error":
            return httpx.Response(503, stream=_Bytes(b""))
        if failing and state.failure == "unreachable":
            raise httpx.ConnectError("unreachable", request=request)
        if "/paths-info/" in path or "/tree/" in path:
            return _json([{"type": "file", "path": "file.bin", "size": len(CONTENT), "oid": "2" * 40}])
        if "/resolve/" in path and request.method == "HEAD":
            return httpx.Response(200, headers={"etag": '"blob"', "content-length": str(len(CONTENT)), "x-repo-commit": SHA})
        if "/resolve/" in path:
            start, end = (int(x) for x in request.headers["range"].removeprefix("bytes=").split("-"))
            data = CONTENT[start : end + 1]
            return httpx.Response(
                206,
                headers={"content-length": str(len(data)), "content-range": f"bytes {start}-{end}/{len(CONTENT)}"},
                stream=_Bytes(data),
            )
        return _json({"id": "team/demo", "sha": SHA})

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)

    async def request(path=FILE, token="Bearer alice", method="GET", data=None):
        async with HTTP_CLIENT(
            transport=httpx.ASGITransport(app=app), base_url="http://olah.test", follow_redirects=True
        ) as client:
            return await client.request(method, path, headers={"authorization": token}, data=data)

    state.request = request
    state.access_dir = os.path.join(config.repos_path, "access", "models", "team", "demo")
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize("path", [FILE, f"/team/demo/resolve/{SHA}/file.bin"])
async def test_verified_caller_is_served_from_cache(env, failure, path):
    assert (await env.request()).content == CONTENT
    env.failure = failure
    env.calls.clear()

    response = await env.request(path)

    assert response.status_code == 200
    assert response.content == CONTENT
    assert response.headers["x-repo-commit"] == SHA
    assert env.calls == [("HEAD", path)]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,status", FAILURES.items())
async def test_unverified_caller_gets_the_upstream_error(env, failure, status):
    assert (await env.request()).status_code == 200
    env.failure = failure

    response = await env.request(token="Bearer mallory")

    assert response.status_code == status


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,status", FAILURES.items())
async def test_cache_miss_gets_the_upstream_error_not_a_404(env, failure, status):
    assert (await env.request()).status_code == 200
    env.failure = failure

    response = await env.request("/team/demo/resolve/main/other.bin")

    assert response.status_code == status
    if status == 429:
        assert response.headers["ratelimit"] == RATE_LIMIT_HEADERS["ratelimit"]


@pytest.mark.asyncio
async def test_record_older_than_stale_if_error_gets_the_upstream_error(env):
    assert (await env.request()).status_code == 200
    stale = time.time() - env.config.metadata_stale_if_error - 1
    for record in os.scandir(env.access_dir):
        with open(record.path, "w", encoding="utf-8") as f:
            f.write(repr(stale))
    env.failure = "rate_limited"

    assert (await env.request()).status_code == 429


@pytest.mark.asyncio
async def test_zero_disables_the_fallback(env):
    env.config.metadata_stale_if_error = 0
    assert (await env.request()).status_code == 200
    env.failure = "rate_limited"

    assert (await env.request()).status_code == 429


@pytest.mark.asyncio
async def test_no_access_is_recorded_when_both_settings_are_zero(env):
    env.config.metadata_stale_if_error = 0
    env.config.metadata_cache_ttl = 0

    assert (await env.request()).status_code == 200
    assert not os.path.exists(env.access_dir)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rate_limited", "server_error"])
async def test_api_routes_fall_back_once_the_metadata_ttl_has_passed(env, failure):
    env.config.metadata_cache_ttl = 0
    tree = f"/api/models/team/demo/tree/{SHA}?recursive=true"
    assert (await env.request(tree)).status_code == 200
    env.failure = failure

    response = await env.request(tree)

    assert response.status_code == 200
    assert response.json()[0]["path"] == "file.bin"


def _age_metadata(repos_path: str, seconds: float) -> None:
    for root, _, files in os.walk(repos_path):
        for name in files:
            path = os.path.join(root, name)
            if os.path.relpath(root, repos_path).split(os.sep)[0] == "access":
                with open(path, "r", encoding="utf-8") as f:
                    confirmed_at = float(f.read())
                with open(path, "w", encoding="utf-8") as f:
                    f.write(repr(confirmed_at - seconds))
            elif name.endswith(".json"):
                with open(path, "r", encoding="utf-8") as f:
                    envelope = json.load(f)
                if "cached_at" in envelope:
                    envelope["cached_at"] -= seconds
                    with open(path, "w", encoding="utf-8") as f:
                        json.dump(envelope, f)


METADATA_REQUESTS = {
    "meta": ("GET", "/api/models/team/demo/revision/main", None),
    "tree": ("GET", "/api/models/team/demo/tree/main?recursive=true", None),
    "paths-info": ("POST", "/api/models/team/demo/paths-info/main", {"paths": ["file.bin"]}),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize("route", METADATA_REQUESTS)
async def test_failed_refresh_of_cached_metadata_serves_the_cached_copy(env, failure, route):
    method, path, data = METADATA_REQUESTS[route]
    fresh = await env.request(path, method=method, data=data)
    assert fresh.status_code == 200
    _age_metadata(env.config.repos_path, env.config.metadata_cache_ttl + 1)
    env.failure = failure
    env.access_checks_succeed = True

    response = await env.request(path, method=method, data=data)

    assert response.status_code == 200
    assert response.content == fresh.content


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled", [False, True])
async def test_failed_refresh_past_stale_if_error_passes_the_error_on(env, disabled):
    method, path, data = METADATA_REQUESTS["tree"]
    assert (await env.request(path, method=method, data=data)).status_code == 200
    if disabled:
        env.config.metadata_stale_if_error = 0
        _age_metadata(env.config.repos_path, env.config.metadata_cache_ttl + 1)
    else:
        _age_metadata(env.config.repos_path, env.config.metadata_stale_if_error + 1)
    env.failure = "rate_limited"
    env.access_checks_succeed = True

    response = await env.request(path, method=method, data=data)

    assert response.status_code == 429
