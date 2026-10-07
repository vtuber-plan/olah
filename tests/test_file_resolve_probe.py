"""Online file routes check access and resolve the commit with one resolve HEAD."""
import hashlib
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from olah.configs import OlahConfig, OlahRuleList
from olah.server_routes import router

HTTP_CLIENT = httpx.AsyncClient
SHA = "1" * 40
CONTENT = b"tiny file content"
LFS_OID = hashlib.sha256(CONTENT).hexdigest()


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
    state = SimpleNamespace(calls=[], head=None, lfs=False, config=config)

    async def upstream(request):
        state.calls.append((request.method, request.url.host, request.url.path))
        path = request.url.path
        if "/paths-info/" in path:
            entry = {"type": "file", "path": "file.bin", "size": len(CONTENT), "oid": "2" * 40}
            if state.lfs:
                entry["lfs"] = {"oid": LFS_OID, "size": len(CONTENT)}
            return httpx.Response(200, json=[entry])
        if path == "/api/models/team/demo":
            return httpx.Response(200)
        if path.endswith("/revision/main"):
            return httpx.Response(200, json={"sha": SHA, "siblings": []})
        if request.method == "HEAD" and "/resolve/" in path:
            state.head_request_headers = dict(request.headers)
            if state.head is not None:
                return state.head(request)
            return httpx.Response(200, headers={"etag": '"blob-oid"', "content-length": str(len(CONTENT)), "x-repo-commit": SHA})
        if request.method == "GET" and ("/resolve/" in path or request.url.host == "cas.invalid"):
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

    async def request(method="GET", path="/team/demo/resolve/main/file.bin"):
        async with HTTP_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
            return await client.request(method, path, headers={"authorization": "Bearer t"})

    state.request = request
    return state


@pytest.mark.asyncio
async def test_branch_download_uses_one_resolve_head_per_request(env):
    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert response.headers["x-repo-commit"] == SHA
    assert [(m, p) for m, _, p in env.calls] == [
        ("HEAD", "/team/demo/resolve/main/file.bin"),
        ("POST", f"/api/models/team/demo/paths-info/{SHA}"),
        ("GET", f"/team/demo/resolve/{SHA}/file.bin"),
    ]
    assert env.head_request_headers["accept-encoding"] == "identity"
    assert env.head_request_headers["authorization"] == "Bearer t"

    env.calls.clear()
    warm = await env.request()

    assert warm.content == CONTENT
    assert [(m, p) for m, _, p in env.calls] == [("HEAD", "/team/demo/resolve/main/file.bin")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,error_code",
    [(401, "RepoNotFound"), (403, "GatedRepo"), (404, "RevisionNotFound"), (404, "EntryNotFound")],
)
async def test_hub_errors_are_passed_through(env, status, error_code):
    env.head = lambda request: httpx.Response(
        status, headers={"x-error-code": error_code, "x-error-message": "nope", "set-cookie": "x"}
    )

    response = await env.request()

    assert response.status_code == status
    assert response.headers["x-error-code"] == error_code
    assert response.headers["x-error-message"] == "nope"
    assert "set-cookie" not in response.headers
    assert len(env.calls) == 1


@pytest.mark.asyncio
async def test_server_error_on_probe_is_a_proxy_timeout(env):
    env.head = lambda request: httpx.Response(503)

    response = await env.request()

    assert response.status_code == 504


@pytest.mark.asyncio
async def test_on_hub_redirects_are_followed_and_cdn_redirect_carries_metadata(env):
    env.lfs = True

    def head(request):
        if request.url.path.startswith("/team/old-name/"):
            return httpx.Response(307, headers={"location": "/team/demo/resolve/main/file.bin"})
        return httpx.Response(
            302,
            headers={
                "location": "https://cas.invalid/signed",
                "x-repo-commit": SHA,
                "x-linked-etag": f'"{LFS_OID}"',
                "x-linked-size": str(len(CONTENT)),
            },
        )

    env.head = head

    response = await env.request("GET", "/team/old-name/resolve/main/file.bin")

    assert response.status_code == 200
    assert response.content == CONTENT
    assert response.headers["etag"] == f'"{LFS_OID}"'
    assert response.headers["x-repo-commit"] == SHA
    heads = [(host, p) for m, host, p in env.calls if m == "HEAD"]
    assert heads == [
        ("upstream.invalid", "/team/old-name/resolve/main/file.bin"),
        ("upstream.invalid", "/team/demo/resolve/main/file.bin"),
    ]


@pytest.mark.asyncio
async def test_proxy_rules_are_checked_before_any_upstream_call(env):
    env.config.proxy = OlahRuleList.from_list([{"repo": "*", "allow": False}])

    response = await env.request()

    assert response.status_code == 401
    assert env.calls == []


@pytest.mark.asyncio
async def test_missing_repo_commit_header_falls_back_to_api_flow(env):
    # A third-party --hf-netloc may not speak the resolve headers; the
    # download must still succeed through the API-based flow.
    env.head = lambda request: httpx.Response(
        200, headers={"etag": '"blob-oid"', "content-length": str(len(CONTENT))}
    )

    response = await env.request()

    assert response.status_code == 200
    assert response.content == CONTENT
    assert [(m, p) for m, _, p in env.calls] == [
        ("HEAD", "/team/demo/resolve/main/file.bin"),
        ("GET", "/api/models/team/demo/revision/main"),
        ("POST", f"/api/models/team/demo/paths-info/{SHA}"),
        # No probe response to reuse, so the etag HEAD happens separately,
        # exactly as in the pre-#97 flow.
        ("HEAD", f"/team/demo/resolve/{SHA}/file.bin"),
        ("GET", f"/team/demo/resolve/{SHA}/file.bin"),
    ]


@pytest.mark.asyncio
async def test_redirect_netloc_compare_ignores_case_and_default_port(env):
    def head(request):
        if request.url.path.startswith("/team/old-name/"):
            # Same host, different case and explicit default port.
            return httpx.Response(
                307, headers={"location": "http://Upstream.Invalid:80/team/demo/resolve/main/file.bin"}
            )
        return httpx.Response(200, headers={"etag": '"blob-oid"', "content-length": str(len(CONTENT)), "x-repo-commit": SHA})

    env.head = head

    response = await env.request("GET", "/team/old-name/resolve/main/file.bin")

    assert response.status_code == 200
    assert response.content == CONTENT
    heads = [p for m, _, p in env.calls if m == "HEAD"]
    assert heads == [
        "/team/old-name/resolve/main/file.bin",
        "/team/demo/resolve/main/file.bin",
    ]
