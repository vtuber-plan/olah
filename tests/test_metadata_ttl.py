"""Metadata TTL reuse: repo visibility, branch -> SHA resolution, refresh.

Pins the v0.5.3 behavior requested in issue #91: repo/revision metadata is
revalidated against Hugging Face at most once per ``metadata-cache-ttl``
instead of on every request, a failed revalidation serves the last known
value, and negative answers are never cached.
"""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.configs import OlahConfig
from olah.proxy.meta import meta_generator
from olah.server_upstream import resolve_requested_commit
from olah.server_access import build_repo_ref
from olah.utils.repo_utils import check_commit_hf, lookup_commit_hf

ORIGINAL_CLIENT = httpx.AsyncClient
SHA1 = "1" * 40
SHA2 = "2" * 40


class _BytesStream(httpx.AsyncByteStream):
    """One-shot body stream: stream= responses survive client.stream() proxying."""

    def __init__(self, data: bytes):
        self.data = data

    async def __aiter__(self):
        yield self.data


@pytest.fixture
def setup(tmp_path, monkeypatch):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.hf_netloc = "upstream.invalid"
    config.metadata_cache_ttl = 600
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    calls = []
    state = {
        "sha": SHA1,
        "revision_status": 200,
        "repo_visible": True,
        "error": False,
        "latency": 0.0,
    }

    async def upstream(request):
        calls.append(request)
        if state["latency"]:
            await asyncio.sleep(state["latency"])
        path = request.url.path
        if path == "/api/models/team/demo":
            return httpx.Response(200 if state["repo_visible"] else 401)
        if path.endswith("/revision/main"):
            if state["error"]:
                raise httpx.ConnectError("upstream unreachable")
            if state["revision_status"] == 429:
                return httpx.Response(
                    429, headers={"x-ratelimit-reset": "10", "retry-after": "10"}
                )
            # stream= so both the non-streaming probe and the streaming
            # api_proxy can consume the same response.
            payload = json.dumps({"sha": state["sha"], "siblings": []}).encode()
            return httpx.Response(
                state["revision_status"],
                headers={"content-type": "application/json"},
                stream=_BytesStream(payload),
            )
        return httpx.Response(404)

    class UpstreamClient(ORIGINAL_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)
    return app, config, calls, state


def _age_every_envelope(repos_path: str, seconds: float) -> None:
    """Rewind ``cached_at`` on every envelope, and access markers, below repos_path by ``seconds``."""
    import os

    for root, _, files in os.walk(repos_path):
        for name in files:
            if os.path.relpath(root, repos_path).split(os.sep)[0] == "access":
                path = os.path.join(root, name)
                with open(path, "r", encoding="utf-8") as f:
                    confirmed_at = float(f.read())
                with open(path, "w", encoding="utf-8") as f:
                    f.write(repr(confirmed_at - seconds))
                continue
            if not name.endswith(".json") or name.endswith(".body"):
                continue
            path = os.path.join(root, name)
            with open(path, "r", encoding="utf-8") as f:
                rq = json.load(f)
            if "cached_at" in rq:
                rq["cached_at"] -= seconds
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(rq, f)


@pytest.mark.asyncio
async def test_visibility_probe_reused_within_ttl(setup):
    app, _, calls, state = setup
    assert await check_commit_hf(app, "models", "team", "demo") is True
    assert await check_commit_hf(app, "models", "team", "demo") is True
    assert len(calls) == 1  # second probe served from the metadata cache

    # The gate still closes once the TTL expires.
    _age_every_envelope(app.state.app_settings.config.repos_path, 601)
    state["repo_visible"] = False
    assert await check_commit_hf(app, "models", "team", "demo") is False
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_negative_visibility_never_cached(setup):
    app, _, calls, state = setup
    state["repo_visible"] = False
    assert await check_commit_hf(app, "models", "team", "demo") is False
    state["repo_visible"] = True
    assert await check_commit_hf(app, "models", "team", "demo") is True
    # A 401 depends on the caller's token; it must not suppress later probes.
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_branch_resolution_reused_within_ttl(setup):
    app, _, calls, state = setup
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    state["sha"] = SHA2  # branch moved upstream; invisible until TTL expiry
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    assert [(r.method, r.url.path) for r in calls] == [
        ("GET", "/api/models/team/demo/revision/main")
    ]

    _age_every_envelope(app.state.app_settings.config.repos_path, 601)
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA2)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_branch_resolution_negative_not_cached(setup):
    app, _, calls, state = setup
    state["revision_status"] = 404
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (False, None)
    state["revision_status"] = 200
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_stale_resolution_served_when_rate_limited(setup):
    app, _, calls, state = setup
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    _age_every_envelope(app.state.app_settings.config.repos_path, 601)
    state["revision_status"] = 429
    # The last known SHA is served instead of a 429: client retries then hit
    # the cache, not what remains of the rate-limit quota.
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_stale_resolution_served_when_upstream_unreachable(setup):
    app, _, calls, state = setup
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    _age_every_envelope(app.state.app_settings.config.repos_path, 601)
    state["error"] = True
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)


@pytest.mark.asyncio
async def test_rate_limit_without_cache_still_raises(setup):
    app, config, calls, state = setup
    state["revision_status"] = 429
    from olah.errors import UpstreamRateLimited

    with pytest.raises(UpstreamRateLimited):
        await lookup_commit_hf(app, "models", "team", "demo", "main")


@pytest.mark.asyncio
async def test_ttl_zero_disables_reuse(setup, tmp_path):
    app, config, calls, _ = setup
    config.metadata_cache_ttl = 0
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    assert await lookup_commit_hf(app, "models", "team", "demo", "main") == (True, SHA1)
    assert await check_commit_hf(app, "models", "team", "demo") is True
    assert await check_commit_hf(app, "models", "team", "demo") is True
    assert len(calls) == 4  # pre-v0.5.3 behavior: every request revalidates
    assert not list(tmp_path.rglob("*.json"))


@pytest.mark.asyncio
async def test_concurrent_resolution_single_flight(setup):
    app, _, calls, state = setup
    state["latency"] = 0.05
    results = await asyncio.gather(
        *[lookup_commit_hf(app, "models", "team", "demo", "main") for _ in range(8)]
    )
    assert all(r == (True, SHA1) for r in results)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_full_request_path_uses_ttl(setup):
    """End-to-end: two branch-addressed resolutions trigger one upstream GET."""
    app, _, calls, _ = setup
    repo = build_repo_ref("models", "team", "demo")
    for _ in range(2):
        resolved, error = await resolve_requested_commit(
            app, repo, "main", None, repo_visible=True
        )
        assert error is None
        assert resolved.resolved == SHA1
    revision_gets = [r for r in calls if r.url.path.endswith("/revision/main")]
    assert len(revision_gets) == 1


@pytest.mark.asyncio
async def test_meta_refresh_respects_ttl(setup):
    """A branch-named metadata request no longer force-refreshes within the TTL."""
    app, config, calls, _ = setup

    async def run(override: bool):
        result = await meta_generator(
            app, "models", "team", "demo", "main",
            override_cache=override, method="GET", authorization=None,
        )
        body = b""
        async for chunk in result.body:
            body += chunk
        return result.status_code, body

    status, body = await run(override=False)
    assert status == 200
    assert json.loads(body) == {"sha": SHA1, "siblings": []}
    calls.clear()
    # prepare_revision_generator refreshes with override_cache=True after a
    # branch resolution; a fresh cache entry makes that refresh a no-op.
    assert (await run(override=True))[0] == 200
    assert calls == []

    _age_every_envelope(config.repos_path, 601)
    assert (await run(override=True))[0] == 200
    assert len(calls) == 1


def test_metadata_cache_ttl_config(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text('[basic]\nmetadata-cache-ttl = 60\n')
    assert OlahConfig(str(config_file)).metadata_cache_ttl == 60
    assert OlahConfig().metadata_cache_ttl == 600  # documented default
