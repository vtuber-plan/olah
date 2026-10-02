"""Revision-addressed cache regressions: immutable SHAs and offline misses."""

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.cache.olah_cache import OlahCache
from olah.configs import OlahConfig, OlahRuleList
from olah.server_api_routes import router as api_router
from olah.server_file_routes import router as file_router
from olah.utils.cache_utils import write_cache_request
from olah.utils.repo_utils import get_commit_hf, get_commit_hf_offline


SHA = "1234567890abcdef" * 2 + "12345678"
REPO = "team/demo"
API = f"/api/models/{REPO}"
FILE_CONTENT = b"abcdefgh"
TREE = [{"type": "file", "oid": "2" * 40, "size": 8, "path": "file.bin"}]
HTTP_CLIENT = httpx.AsyncClient


class _BytesStream(httpx.AsyncByteStream):
    def __init__(self, content):
        self.content = content

    async def __aiter__(self):
        yield self.content


@pytest.fixture
def environment(tmp_path, monkeypatch):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.mirrors_path = []
    config.hf_netloc = "upstream.invalid"
    config.cache_block_size = 4
    config.cache_chunk_size = 4
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(api_router)
    app.include_router(file_router)
    calls = []
    upstream_status = {}

    async def upstream(request):
        calls.append((request.method, request.url.path, request.headers.get("authorization")))
        if "/tree/" in request.url.path or "/paths-info/" in request.url.path:
            content = json.dumps(TREE).encode()
        elif "/commits/" in request.url.path:
            content = json.dumps([{"id": SHA}]).encode()
        elif "/resolve/" in request.url.path:
            content = FILE_CONTENT
            if "range" in request.headers:
                start, end = request.headers["range"].removeprefix("bytes=").split("-")
                content = content[int(start) : int(end) + 1]
        else:
            content = json.dumps({"id": REPO, "sha": SHA}).encode()
        if request.method == "HEAD":
            content = b""
        return httpx.Response(
            upstream_status.get(request.url.path, 200),
            headers={"content-type": "application/json", "content-length": str(len(content)), "etag": '"file-etag"'},
            stream=_BytesStream(content),
            request=request,
        )

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)
    return SimpleNamespace(app=app, config=config, calls=calls, statuses=upstream_status)


async def _cache(env, relative_path, payload):
    path = Path(env.config.repos_path) / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    await write_cache_request(str(path), 200, {"content-type": "application/json"}, json.dumps(payload).encode())


async def _branch_metadata(env):
    await _cache(env, f"api/models/{REPO}/revision/main/meta_get.json", {"sha": SHA})


async def _tree_cache(env):
    await _cache(env, f"api/models/{REPO}/tree/{SHA}/tree_get_recursive_True_expand_False.json", TREE)


async def _request(env, path, method="GET", **kwargs):
    async with HTTP_CLIENT(transport=httpx.ASGITransport(app=env.app), base_url="http://olah.test", follow_redirects=True) as client:
        return await client.request(method, path, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("offline", [False, True])
@pytest.mark.parametrize("sha", [SHA, SHA.upper()])
async def test_full_sha_resolution_needs_no_metadata(environment, offline, sha):
    env = environment
    env.config.offline = offline
    assert await get_commit_hf(env.app, "models", "team", "demo", sha) == sha.lower()
    assert await get_commit_hf_offline(env.app, "models", "team", "demo", sha) == sha.lower()
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", ["main", "a" * 39, "a" * 41, "g" * 40, SHA + "\n"])
async def test_only_full_hex_sha_bypasses_alias_metadata(environment, revision):
    env = environment
    env.config.offline = True
    assert await get_commit_hf(env.app, "models", "team", "demo", revision) is None
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("offline", [False, True])
@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("sha", [SHA, SHA.upper()])
async def test_full_sha_tree_cold_and_warm_online_and_offline(environment, offline, warm, sha):
    env = environment
    env.config.offline = offline
    if warm:
        await _tree_cache(env)
    response = await _request(env, f"{API}/tree/{sha}?recursive=true", headers={"authorization": "Bearer fixture"})
    if offline and not warm:
        assert response.status_code == 404
        assert response.headers["x-error-code"] == "EntryNotFound"
    else:
        assert response.status_code == 200
        assert response.json() == TREE
    if offline:
        assert env.calls == []
    else:
        assert env.calls[:2] == [("HEAD", API, "Bearer fixture"), ("HEAD", f"{API}/revision/{sha}", "Bearer fixture")]
        assert all(method == "HEAD" or "/tree/" in path for method, path, _ in env.calls)
        assert len(env.calls) == (2 if warm else 4)
        assert all(path == f"{API}/tree/{SHA}/" for method, path, _ in env.calls if method != "HEAD")
    assert not (Path(env.config.repos_path) / f"api/models/{REPO}/revision/{SHA}/meta_get.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("warm", [False, True])
async def test_offline_branch_metadata_does_not_allow_tree_network(environment, warm):
    env = environment
    env.config.offline = True
    await _branch_metadata(env)
    if warm:
        await _tree_cache(env)
    response = await _request(env, f"{API}/tree/main/?recursive=true")
    assert response.status_code == (200 if warm else 404)
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [SHA, "main"])
@pytest.mark.parametrize("endpoint,method", [("revision", "GET"), ("revision", "HEAD"), ("commits", "GET"), ("commits", "HEAD"), ("tree", "HEAD"), ("paths-info", "POST")])
async def test_offline_missing_api_payload_never_calls_upstream(environment, revision, endpoint, method):
    env = environment
    env.config.offline = True
    await _branch_metadata(env)
    kwargs = {"data": {"paths": "file.bin"}} if endpoint == "paths-info" else {}
    suffix = "/" if endpoint == "tree" else ""
    response = await _request(env, f"{API}/{endpoint}/{revision}{suffix}", method, **kwargs)
    assert response.status_code == 404
    assert response.headers["x-error-code"] == "EntryNotFound"
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("offline", [False, True])
async def test_full_sha_cached_tree_cannot_bypass_proxy_rules(environment, offline):
    env = environment
    env.config.offline = offline
    env.config.proxy = OlahRuleList.from_list([{"repo": "*", "allow": False}])
    await _tree_cache(env)
    response = await _request(env, f"{API}/tree/{SHA}/?recursive=true")
    assert response.status_code == 401
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 404])
async def test_full_sha_cached_tree_cannot_bypass_upstream_visibility(environment, status):
    env = environment
    env.statuses[API] = status
    await _tree_cache(env)
    response = await _request(env, f"{API}/tree/{SHA}/?recursive=true", headers={"authorization": "Bearer denied"})
    assert response.status_code == 401
    assert env.calls == [("HEAD", API, "Bearer denied")]


@pytest.mark.asyncio
async def test_full_sha_still_checks_revision_visibility(environment):
    env = environment
    env.statuses[f"{API}/revision/{SHA}"] = 404
    await _tree_cache(env)
    response = await _request(env, f"{API}/tree/{SHA}/?recursive=true")
    assert response.status_code == 404
    assert response.headers["x-error-code"] == "RevisionNotFound"
    assert [method for method, _, _ in env.calls] == ["HEAD", "HEAD"]


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [SHA, "main"])
@pytest.mark.parametrize("warm_blocks,range_header,expected_status,expected_body", [(0, None, 404, b""), (1, None, 404, b""), (1, "bytes=0-3", 206, b"abcd"), (1, "bytes=4-7", 404, b""), (2, None, 200, FILE_CONTENT)])
async def test_offline_resolve_only_serves_cached_ranges(environment, revision, warm_blocks, range_header, expected_status, expected_body):
    env = environment
    env.config.offline = True
    if revision == "main":
        await _branch_metadata(env)
    await _cache(env, f"api/models/{REPO}/paths-info/{SHA}/file.bin/paths-info_post.json", TREE)
    if warm_blocks:
        path = str(Path(env.config.repos_path) / f"files/models/{REPO}/resolve/{SHA}/file.bin")
        cache = OlahCache(path, file_size=8, block_size=4, chunk_size=4)
        try:
            for block in range(warm_blocks):
                await cache.write_block(block, FILE_CONTENT[4 * block : 4 * block + 4])
        finally:
            cache.close()
    headers = {"range": range_header} if range_header else {}
    response = await _request(env, f"/{REPO}/resolve/{revision}/file.bin", headers=headers)
    assert response.status_code == expected_status
    assert response.content == expected_body
    assert env.calls == []


@pytest.mark.asyncio
async def test_offline_resolve_does_not_probe_redirect_model(environment):
    env = environment
    env.config.offline = True
    env.config.cache_redirect_model = True
    await _branch_metadata(env)
    response = await _request(env, f"/{REPO}/resolve/main/file.bin")
    assert response.status_code == 404
    assert env.calls == []


@pytest.mark.asyncio
async def test_online_tree_cache_is_reusable_offline_without_revision_metadata(environment):
    env = environment
    path = f"{API}/tree/{SHA}/?recursive=true"
    online = await _request(env, path)
    assert online.status_code == 200
    assert online.json() == TREE
    env.config.offline = True
    env.calls.clear()
    offline = await _request(env, path)
    assert offline.status_code == 200
    assert offline.json() == TREE
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint,method,relative_path,payload", [
    ("revision", "GET", "revision/{sha}/meta_get.json", {"sha": SHA}),
    ("commits", "GET", "commits/{sha}/commits_get.json", [{"id": SHA}]),
    ("paths-info", "POST", "paths-info/{sha}/file.bin/paths-info_post.json", TREE),
])
async def test_full_sha_offline_api_cache_hits(environment, endpoint, method, relative_path, payload):
    env = environment
    env.config.offline = True
    await _cache(env, f"api/models/{REPO}/" + relative_path.format(sha=SHA), payload)
    kwargs = {"data": {"paths": "file.bin"}} if endpoint == "paths-info" else {}
    response = await _request(env, f"{API}/{endpoint}/{SHA}", method, **kwargs)
    assert response.status_code == 200
    assert response.json() == payload
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["meta", "commits", "tree", "pathsinfo"])
async def test_offline_generator_override_still_uses_cache(environment, kind):
    from olah.proxy.commits import commits_generator
    from olah.proxy.meta import meta_generator
    from olah.proxy.pathsinfo import pathsinfo_generator
    from olah.proxy.tree import tree_generator

    env = environment
    env.config.offline = True
    await _cache(env, f"api/models/{REPO}/revision/{SHA}/meta_get.json", {"sha": SHA})
    await _cache(env, f"api/models/{REPO}/commits/{SHA}/commits_get.json", [{"id": SHA}])
    await _cache(env, f"api/models/{REPO}/paths-info/{SHA}/file.bin/paths-info_post.json", TREE)
    await _tree_cache(env)
    generator, extra = {
        "meta": (meta_generator, {}),
        "commits": (commits_generator, {}),
        "tree": (tree_generator, {"path": "", "recursive": True, "expand": False}),
        "pathsinfo": (pathsinfo_generator, {"paths": ["file.bin"]}),
    }[kind]
    result = await generator(
        app=env.app, repo_type="models", org="team", repo="demo", commit=SHA,
        override_cache=True, method="post" if kind == "pathsinfo" else "get",
        authorization=None, **extra,
    )
    assert result.status_code == 200
    assert [chunk async for chunk in result.body]
    assert env.calls == []


@pytest.mark.asyncio
async def test_offline_pathsinfo_partial_cache_is_a_miss_without_network(environment):
    env = environment
    env.config.offline = True
    await _cache(env, f"api/models/{REPO}/paths-info/{SHA}/file.bin/paths-info_post.json", TREE)
    response = await _request(env, f"{API}/paths-info/{SHA}", "POST", data={"paths": ["file.bin", "missing.bin"]})
    assert response.status_code == 404
    assert env.calls == []


@pytest.mark.asyncio
async def test_full_sha_online_respects_cache_write_rules(environment):
    env = environment
    env.config.cache = OlahRuleList.from_list([{"repo": "*", "allow": False}])
    response = await _request(env, f"{API}/tree/{SHA}/?recursive=true")
    assert response.status_code == 200
    assert not list(Path(env.config.repos_path).rglob("tree_*.json"))
    assert len(env.calls) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("warm", [False, True])
async def test_offline_file_head_only_requires_cached_path_metadata(environment, warm):
    env = environment
    env.config.offline = True
    if warm:
        await _cache(env, f"api/models/{REPO}/paths-info/{SHA}/file.bin/paths-info_post.json", TREE)
    response = await _request(env, f"/{REPO}/resolve/{SHA}/file.bin", "HEAD")
    assert response.status_code == (200 if warm else 404)
    if warm:
        assert response.headers["content-length"] == "8"
        assert response.headers["x-repo-commit"] == SHA
    assert env.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_offline_stream_never_recovers_blocks_from_upstream(environment, corrupt):
    from olah.cache.olah_cache import CacheIntegrityError
    from olah.proxy.files import _file_chunk_get

    env = environment
    env.config.offline = True
    path = Path(env.config.repos_path) / "stream-cache"
    cache = OlahCache(str(path), file_size=8, block_size=4, chunk_size=4)
    try:
        if corrupt:
            await cache.write_block(0, FILE_CONTENT[:4])
            Path(cache.get_block_path(0)).write_bytes(b"bad!")
    finally:
        cache.close()
    async with httpx.AsyncClient() as client:
        body = _file_chunk_get(
            app=env.app, save_path=str(path), client=client, method="GET",
            url="https://upstream.invalid/file.bin", headers={"range": "bytes=0-3"},
            allow_cache=True, file_size=8,
        )
        with pytest.raises(
            CacheIntegrityError if corrupt else Exception,
            match=None if corrupt else "cache miss in cache-only mode",
        ):
            _ = [chunk async for chunk in body]
    assert env.calls == []
