import importlib
import sys
import types
from types import SimpleNamespace

import httpx
import pytest
from fastapi import Request


def _ensure_portalocker_stub():
    if "portalocker" in sys.modules:
        return
    portalocker_stub = types.ModuleType("portalocker")

    class _Lock:
        def __init__(self, *args, **kwargs):
            self._fh = open(args[0], kwargs.get("mode", "a+b"))

        def __enter__(self):
            return self._fh

        def __exit__(self, exc_type, exc, tb):
            self._fh.close()
            return False

    portalocker_stub.Lock = _Lock
    portalocker_stub.LOCK_EX = 1
    portalocker_stub.LOCK_SH = 2
    sys.modules["portalocker"] = portalocker_stub


_ensure_portalocker_stub()
sys.modules.pop("olah.proxy.files", None)
proxy_files = importlib.import_module("olah.proxy.files")
server_api_routes = importlib.import_module("olah.server_api_routes")


def _fake_app(tmp_path=None, offline=False):
    config = SimpleNamespace(
        offline=offline,
        hf_netloc="huggingface.co",
        hf_lfs_netloc="cdn-lfs.huggingface.co",
        repos_path=str(tmp_path / "repos") if tmp_path is not None else "/tmp/olah-tests",
        hf_url_base=lambda: "https://huggingface.co",
        hf_lfs_url_base=lambda: "https://cdn-lfs.huggingface.co",
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


def _make_request(method="GET", headers=None, path="/api/test", app=None):
    raw_headers = []
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode("ascii"), value.encode("ascii")))
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 18090),
        "app": app,
    }
    return Request(scope)


def _patch_async_client(monkeypatch, module, response=None, raises=None, captured=None):
    """Replace `module.httpx.AsyncClient` with a stub returning `response` (or raising)."""

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, *args, **kwargs):
            if captured is not None:
                captured.append(kwargs)
            if raises is not None:
                raise raises
            return response

    monkeypatch.setattr(module.httpx, "AsyncClient", _FakeAsyncClient)


# ---------------------------------------------------------------------------
# _remote_file_metadata
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remote_file_metadata_captures_xet_headers(monkeypatch):
    fake_response = SimpleNamespace(
        status_code=302,
        headers={
            "x-xet-hash": "abc123",
            "x-linked-size": "53121272560",
            "x-linked-etag": '"deadbeef"',
            "link": '<https://huggingface.co/api/models/x/y/xet-read-token/main>; rel="xet-auth"',
        },
    )
    _patch_async_client(monkeypatch, proxy_files, response=fake_response)

    metadata, status = await proxy_files._remote_file_metadata(
        app=_fake_app(),
        hf_url="https://huggingface.co/x/y/resolve/main/big.bin",
        authorization=None,
        offline=False,
    )

    assert metadata is not None
    assert status == 302
    assert metadata.file_size == 53121272560  # falls back to x-linked-size
    assert metadata.etag == '"deadbeef"'  # falls back to x-linked-etag
    assert metadata.xet_headers is not None
    assert metadata.xet_headers["x-xet-hash"] == "abc123"
    assert "link" in metadata.xet_headers


@pytest.mark.asyncio
async def test_remote_file_metadata_no_xet_headers_for_plain_files(monkeypatch):
    fake_response = SimpleNamespace(
        status_code=200,
        headers={"content-length": "42", "etag": '"plain-etag"'},
    )
    _patch_async_client(monkeypatch, proxy_files, response=fake_response)

    metadata, status = await proxy_files._remote_file_metadata(
        app=_fake_app(),
        hf_url="https://huggingface.co/x/y/resolve/main/small.txt",
        authorization=None,
        offline=False,
    )

    assert metadata is not None
    assert status == 200
    assert metadata.file_size == 42
    assert metadata.etag == '"plain-etag"'
    assert metadata.xet_headers is None


# ---------------------------------------------------------------------------
# _probe_xet_resolve (the shared upstream HEAD)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_xet_resolve_returns_none_on_http_error(monkeypatch):
    _patch_async_client(monkeypatch, proxy_files, raises=httpx.ConnectError("refused"))

    response = await proxy_files._probe_xet_resolve(
        hf_url="http://localhost:1/file",
        authorization=None,
    )
    assert response is None


@pytest.mark.asyncio
async def test_probe_xet_resolve_forwards_authorization(monkeypatch):
    captured = []
    fake_response = SimpleNamespace(status_code=200, headers={})
    _patch_async_client(monkeypatch, proxy_files, response=fake_response, captured=captured)

    response = await proxy_files._probe_xet_resolve(
        hf_url="https://huggingface.co/x/y/resolve/main/file",
        authorization="Bearer token",
    )

    assert response is fake_response
    assert captured[0]["headers"].get("authorization") == "Bearer token"
    assert captured[0]["follow_redirects"] is False


@pytest.mark.asyncio
async def test_probe_xet_resolve_follows_relative_redirect(monkeypatch):
    # When upstream returns a relative 307 (e.g. canonical-name redirect),
    # the probe must follow it to find the response that actually carries
    # x-xet-hash. Otherwise renamed repos quietly fall back to the
    # legacy "file too large" path.
    visited = []

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, **kwargs):
            visited.append(url)
            if url.endswith("/old-name/repo/resolve/main/file.bin"):
                return SimpleNamespace(
                    status_code=307,
                    headers={"location": "/new-name/repo/resolve/main/file.bin"},
                )
            return SimpleNamespace(
                status_code=302,
                headers={
                    "x-xet-hash": "abc",
                    "location": "https://cas-bridge.xethub.hf.co/x?signed=1",
                },
            )

    monkeypatch.setattr(proxy_files.httpx, "AsyncClient", _FakeAsyncClient)

    response = await proxy_files._probe_xet_resolve(
        hf_url="https://huggingface.co/old-name/repo/resolve/main/file.bin",
        authorization=None,
    )

    assert response is not None
    assert response.headers["x-xet-hash"] == "abc"
    assert visited == [
        "https://huggingface.co/old-name/repo/resolve/main/file.bin",
        "https://huggingface.co/new-name/repo/resolve/main/file.bin",
    ]


@pytest.mark.asyncio
async def test_probe_xet_resolve_stops_at_absolute_redirect(monkeypatch):
    # An absolute 3xx is the *Xet* redirect itself — stop there, do not follow.
    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        call_count = 0

        async def request(self, method, url, **kwargs):
            type(self).call_count += 1
            if type(self).call_count > 1:
                raise AssertionError("absolute redirect must not be followed")
            return SimpleNamespace(
                status_code=302,
                headers={
                    "x-xet-hash": "h",
                    "location": "https://cas-bridge.xethub.hf.co/abs?signed=1",
                },
            )

    monkeypatch.setattr(proxy_files.httpx, "AsyncClient", _FakeAsyncClient)

    response = await proxy_files._probe_xet_resolve(
        hf_url="https://huggingface.co/x/y/resolve/main/big.bin",
        authorization=None,
    )
    assert response is not None
    assert response.headers["x-xet-hash"] == "h"


@pytest.mark.asyncio
async def test_probe_xet_resolve_resolves_redirects_via_rfc3986(monkeypatch):
    # A relative redirect must replace the base URL's query string, not
    # carry it over. urlparse._replace would be wrong here; urljoin is right.
    visited = []

    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, **kwargs):
            visited.append(url)
            if "?old=1" in url:
                return SimpleNamespace(
                    status_code=307,
                    headers={"location": "/new/path"},
                )
            return SimpleNamespace(
                status_code=302,
                headers={"x-xet-hash": "h", "location": "https://cas/x"},
            )

    monkeypatch.setattr(proxy_files.httpx, "AsyncClient", _FakeAsyncClient)

    response = await proxy_files._probe_xet_resolve(
        hf_url="https://huggingface.co/old/path?old=1",
        authorization=None,
    )

    assert response is not None
    assert visited == [
        "https://huggingface.co/old/path?old=1",
        "https://huggingface.co/new/path",  # query string dropped, not carried
    ]


@pytest.mark.asyncio
async def test_probe_xet_resolve_caps_redirect_loop(monkeypatch):
    # A pathological infinite redirect loop must not hang the probe.
    class _FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, **kwargs):
            return SimpleNamespace(
                status_code=307,
                headers={"location": "/loop"},
            )

    monkeypatch.setattr(proxy_files.httpx, "AsyncClient", _FakeAsyncClient)

    response = await proxy_files._probe_xet_resolve(
        hf_url="https://huggingface.co/loop",
        authorization=None,
    )
    assert response is None


# ---------------------------------------------------------------------------
# _xet_passthrough_result (pure decision from a probed response)
# ---------------------------------------------------------------------------


def _xet_response(size=str(100 * 1024**3)):
    return SimpleNamespace(
        status_code=302,
        headers={
            "x-xet-hash": "bdcc...0f6d",
            "x-linked-size": size,
            "x-linked-etag": '"caa3..."',
            "etag": '"caa3..."',
            "link": '<https://huggingface.co/api/models/x/y/xet-read-token/abc>; rel="xet-auth"',
            "x-repo-commit": "abc123",
            "location": "https://cas-bridge.xethub.hf.co/xet-bridge-us/abc?signed=1",
        },
    )


def test_passthrough_result_none_when_no_xet_hash():
    plain = SimpleNamespace(
        status_code=302,
        headers={"location": "https://cdn-lfs.huggingface.co/.../small.bin", "content-length": "100"},
    )
    assert proxy_files._xet_passthrough_result(plain, "main", 0) is None


def test_passthrough_result_none_when_probe_failed():
    assert proxy_files._xet_passthrough_result(None, "main", 0) is None


def test_passthrough_result_mirrors_upstream_xet_headers():
    result = proxy_files._xet_passthrough_result(_xet_response(), None, 0)

    assert result is not None
    assert result.status_code == 302
    headers = result.headers
    assert headers["x-xet-hash"] == "bdcc...0f6d"
    assert headers["x-linked-size"] == str(100 * 1024**3)
    assert headers["x-linked-etag"] == '"caa3..."'
    assert "link" in headers
    assert headers["accept-ranges"] == "bytes"
    # Location must be forwarded so huggingface_hub's relative-redirect follower
    # doesn't KeyError on the 3xx response.
    assert headers["location"].startswith("https://cas-bridge.xethub.hf.co/")
    # We must NOT synthesize content-length from x-linked-size: the response
    # body is empty and x-linked-size is the *target file* size, not the body.
    # Lying about it would mislead any client that follows the 302.
    assert "content-length" not in headers


@pytest.mark.asyncio
async def test_passthrough_result_body_is_single_empty_chunk():
    result = proxy_files._xet_passthrough_result(_xet_response(), None, 0)
    assert [chunk async for chunk in result.body] == [b""]


def test_passthrough_result_uses_explicit_commit_over_upstream():
    response = SimpleNamespace(
        status_code=302,
        headers={"x-xet-hash": "h", "x-repo-commit": "upstream-commit"},
    )
    result = proxy_files._xet_passthrough_result(response, "explicit-commit", 0)
    assert result is not None
    # When the caller passes a commit, it should win over the upstream value.
    assert result.headers["x-repo-commit"] == "explicit-commit"


def test_passthrough_result_gates_on_min_size():
    # 100 GiB file with a 50 GiB threshold: passes through.
    assert proxy_files._xet_passthrough_result(_xet_response(), None, 50 * 1024**3) is not None
    # Same file with a threshold above its size: stays on the cache path.
    assert proxy_files._xet_passthrough_result(_xet_response(), None, 200 * 1024**3) is None
    # Exactly at the threshold passes through (>= semantics).
    assert proxy_files._xet_passthrough_result(_xet_response(), None, 100 * 1024**3) is not None


def test_passthrough_result_size_unknown_still_passes_through():
    # No x-linked-size / content-length at all: assume the client needs the
    # native Xet path rather than risking hf_hub's "file too large" later.
    response = SimpleNamespace(
        status_code=302,
        headers={"x-xet-hash": "h"},
    )
    assert proxy_files._xet_passthrough_result(response, None, 50 * 1024**3) is not None


# ---------------------------------------------------------------------------
# _file_realtime_stream gating (opt-in, threshold, single probe)
# ---------------------------------------------------------------------------


def _fake_app_xet(tmp_path, *, offline=False, xet_passthrough=True, redirect_model=False, min_size=0):
    config = SimpleNamespace(
        offline=offline,
        xet_passthrough=xet_passthrough,
        xet_passthrough_min_size=min_size,
        cache_redirect_model=redirect_model,
        hf_netloc="huggingface.co",
        hf_lfs_netloc="cdn-lfs.huggingface.co",
        repos_path=str(tmp_path / "repos"),
        hf_url_base=lambda: "https://huggingface.co",
        hf_lfs_url_base=lambda: "https://cdn-lfs.huggingface.co",
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


def _stream_kwargs(tmp_path, **extra):
    kwargs = dict(
        repo_type="models",
        org="x",
        repo="y",
        file_path="big.bin",
        save_path=str(tmp_path / "save"),
        url="https://huggingface.co/x/y/resolve/main/big.bin",
        request=_make_request(),
        method="GET",
        allow_cache=True,
        commit="main",
    )
    kwargs.update(extra)
    return kwargs


@pytest.mark.asyncio
async def test_file_realtime_stream_short_circuits_on_xet_when_enabled(monkeypatch, tmp_path):
    async def fail_pathsinfo(*args, **kwargs):
        raise AssertionError("pathsinfo_generator must not run for passed-through Xet files")

    async def fake_probe(hf_url, authorization):
        return _xet_response()

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fail_pathsinfo)
    monkeypatch.setattr(proxy_files, "_probe_xet_resolve", fake_probe)

    result = await proxy_files._file_realtime_stream(
        app=_fake_app_xet(tmp_path, xet_passthrough=True, min_size=0),
        **_stream_kwargs(tmp_path),
    )

    assert result is not None
    assert result.status_code == 302
    assert result.headers["x-xet-hash"] == "bdcc...0f6d"


@pytest.mark.asyncio
async def test_file_realtime_stream_skips_probe_when_passthrough_disabled(monkeypatch, tmp_path):
    # Default config (both features off): no upstream probe at all — the
    # resolve hot path pays zero extra HEADs.
    probe_calls = []

    async def fail_probe(*args, **kwargs):
        probe_calls.append(True)
        raise AssertionError("no probe may run when both Xet routes are off")

    async def fake_pathsinfo_generator(*args, **kwargs):
        return proxy_files.ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=proxy_files.single_chunk_body('[{"size": 5}]'),
        )

    async def fake_etag(*args, **kwargs):
        return '"some-etag"'

    monkeypatch.setattr(proxy_files, "_probe_xet_resolve", fail_probe)
    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_etag)

    await proxy_files._file_realtime_stream(
        app=_fake_app_xet(tmp_path, offline=False, xet_passthrough=False, redirect_model=False),
        **_stream_kwargs(tmp_path, file_path="file.txt", url="https://huggingface.co/x/y/resolve/main/file.txt"),
    )

    assert probe_calls == []


@pytest.mark.asyncio
async def test_file_realtime_stream_skips_probe_when_offline(monkeypatch, tmp_path):
    # Offline mode must bypass the upstream probe entirely.
    probe_calls = []

    async def fail_probe(*args, **kwargs):
        probe_calls.append(True)
        raise AssertionError("no probe may run offline")

    async def fake_pathsinfo_generator(*args, **kwargs):
        return proxy_files.ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=proxy_files.single_chunk_body('[{"size": 5}]'),
        )

    async def fake_etag(*args, **kwargs):
        return '"some-etag"'

    monkeypatch.setattr(proxy_files, "_probe_xet_resolve", fail_probe)
    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_etag)

    await proxy_files._file_realtime_stream(
        app=_fake_app_xet(tmp_path, offline=True),
        **_stream_kwargs(tmp_path, file_path="file.txt", method="HEAD", request=_make_request(method="HEAD")),
    )

    assert probe_calls == []


@pytest.mark.asyncio
async def test_file_realtime_stream_small_xet_file_falls_through_to_cache(monkeypatch, tmp_path):
    # Pass-through enabled, but the Xet file is below the threshold: it must
    # NOT be handed to hf_xet — it keeps flowing through olah's normal
    # (cacheable) path.
    async def fake_probe(hf_url, authorization):
        return _xet_response(size=str(1024))  # 1 KB Xet file

    async def fake_pathsinfo_generator(*args, **kwargs):
        return proxy_files.ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=proxy_files.single_chunk_body('[{"path": "big.bin", "size": 1024}]'),
        )

    async def fake_etag(*args, **kwargs):
        return '"some-etag"'

    async def fake_chunk_get(**kwargs):
        yield b"tiny"

    monkeypatch.setattr(proxy_files, "_probe_xet_resolve", fake_probe)
    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_get", fake_chunk_get)

    result = await proxy_files._file_realtime_stream(
        app=_fake_app_xet(tmp_path, xet_passthrough=True, min_size=50 * 1024**3),
        **_stream_kwargs(tmp_path),
    )

    # Falls through to the normal cacheable flow, not a 302 Xet response.
    assert result.status_code == 200
    assert "x-xet-hash" not in result.headers


@pytest.mark.asyncio
async def test_file_realtime_stream_probes_upstream_only_once(monkeypatch, tmp_path):
    # Both routes enabled: exactly ONE upstream HEAD serves both decisions.
    probe_calls = []

    async def fake_probe(hf_url, authorization):
        probe_calls.append(hf_url)
        # A classic LFS redirect: neither route engages, both consume the probe.
        return SimpleNamespace(
            status_code=302,
            headers={"location": "https://cdn-lfs.huggingface.co/rep/oid"},
        )

    async def fake_pathsinfo_generator(*args, **kwargs):
        return proxy_files.ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=proxy_files.single_chunk_body('[{"path": "f.bin", "size": 4}]'),
        )

    async def fake_etag(*args, **kwargs):
        return '"some-etag"'

    async def fake_chunk_get(**kwargs):
        yield b"data"

    monkeypatch.setattr(proxy_files, "_probe_xet_resolve", fake_probe)
    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_get", fake_chunk_get)

    result = await proxy_files._file_realtime_stream(
        app=_fake_app_xet(tmp_path, xet_passthrough=True, min_size=0, redirect_model=True),
        **_stream_kwargs(tmp_path),
    )

    assert len(probe_calls) == 1
    assert result.status_code == 200


# ---------------------------------------------------------------------------
# xet-read-token route forwarder
# ---------------------------------------------------------------------------


def _allow_repo_access(monkeypatch):
    """Bypass olah's repo proxy/visibility gates by stubbing them to allow."""
    async def _ok(*args, **kwargs):
        return None
    monkeypatch.setattr(server_api_routes, "ensure_repo_visibility", _ok)


@pytest.mark.asyncio
async def test_xet_read_token_passthrough_forwards_request(monkeypatch):
    _allow_repo_access(monkeypatch)
    captured = []
    fake_response = SimpleNamespace(
        status_code=200,
        headers={
            "content-type": "application/json",
            "content-length": "123",
            "content-encoding": "gzip",
        },
        content=b'{"casUrl":"https://cas-bridge.xethub.hf.co","accessToken":"jwt","exp":1}',
    )
    _patch_async_client(monkeypatch, server_api_routes, response=fake_response, captured=captured)

    app = _fake_app()
    request = _make_request(
        method="GET",
        headers={"authorization": "Bearer abc", "user-agent": "huggingface_hub/x"},
        path="/api/models/x/y/xet-read-token/abcdef",
        app=app,
    )

    response = await server_api_routes._xet_read_token_passthrough(
        repo_type="models",
        org_repo="x/y",
        commit="abcdef",
        request=request,
    )

    assert response.status_code == 200
    assert response.body == fake_response.content
    # content-encoding must be stripped (we already decoded, re-sending it would
    # mislead clients into double-decoding). FastAPI computes its own
    # content-length from the body, so we only check the upstream "123" was not
    # echoed verbatim.
    response_headers_lower = {k.lower(): v for k, v in response.headers.items()}
    assert "content-encoding" not in response_headers_lower
    assert response_headers_lower.get("content-length") != "123"
    assert response_headers_lower["content-length"] == str(len(fake_response.content))
    # Upstream URL must be the official endpoint with the right path.
    assert captured[0]["url"] == "https://huggingface.co/api/models/x/y/xet-read-token/abcdef"
    assert captured[0]["method"] == "GET"
    # Authorization and Host must be forwarded.
    assert captured[0]["headers"]["authorization"] == "Bearer abc"
    assert captured[0]["headers"]["host"] == "huggingface.co"


@pytest.mark.asyncio
async def test_xet_read_token_passthrough_returns_504_on_http_error(monkeypatch):
    _allow_repo_access(monkeypatch)
    _patch_async_client(monkeypatch, server_api_routes, raises=httpx.ConnectError("boom"))

    response = await server_api_routes._xet_read_token_passthrough(
        repo_type="datasets",
        org_repo="x/y",
        commit="abcdef",
        request=_make_request(method="GET", path="/api/...", app=_fake_app()),
    )

    assert response.status_code == 504


@pytest.mark.asyncio
async def test_xet_read_token_passthrough_enforces_repo_access(monkeypatch):
    # If olah's policy blocks the repo, the route must NOT forward the request
    # upstream — otherwise olah would silently relay an authenticated token
    # request for a repo the mirror is configured to refuse.
    upstream_called = []

    async def _deny(*args, **kwargs):
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "blocked"}, status_code=403)

    monkeypatch.setattr(server_api_routes, "ensure_repo_visibility", _deny)
    _patch_async_client(
        monkeypatch,
        server_api_routes,
        captured=upstream_called,
        response=SimpleNamespace(status_code=200, headers={}, content=b""),
    )

    response = await server_api_routes._xet_read_token_passthrough(
        repo_type="models",
        org_repo="blocked/repo",
        commit="abcdef",
        request=_make_request(
            method="GET",
            headers={"authorization": "Bearer abc"},
            path="/api/models/blocked/repo/xet-read-token/abcdef",
            app=_fake_app(),
        ),
    )

    assert response.status_code == 403
    assert upstream_called == []  # no upstream call was made
