"""Hub refs routing, Git semantics, mutable snapshots and offline boundaries."""
import json
from types import SimpleNamespace

import git
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from huggingface_hub import HfApi

from olah.configs import OlahConfig, OlahRuleList
from olah.mirror.repos import LocalMirrorRepo
from olah.server_api_routes import router

ORIGINAL_CLIENT = httpx.AsyncClient
SHA1 = "1" * 40
SHA2 = "2" * 40


def refs_payload(sha=SHA1, include_prs=False):
    result = {"branches": [{"name": "main", "ref": "refs/heads/main", "targetCommit": sha}], "tags": [], "converts": []}
    if include_prs:
        result["pullRequests"] = [{"name": "1", "ref": "refs/pr/1", "targetCommit": sha}]
    return result


@pytest.fixture
def setup(tmp_path, monkeypatch):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.hf_netloc = "upstream.invalid"
    app = FastAPI()
    app.include_router(router)
    app.state.app_settings = SimpleNamespace(config=config)
    calls = []
    state = {"sha": SHA1, "status": 200, "visible": True}

    async def upstream(request):
        calls.append(request)
        if request.method == "HEAD":
            return httpx.Response(200 if state["visible"] else 401)
        assert request.url.path.endswith("/refs")
        if state.get("error"):
            raise httpx.ConnectError("offline upstream")
        if state.get("invalid"):
            return httpx.Response(200, content=b"not json")
        return httpx.Response(state["status"], json=refs_payload(state["sha"], request.url.params.get("include_prs") == "1"))

    class UpstreamClient(ORIGINAL_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)
    return app, config, calls, state


@pytest.mark.asyncio
@pytest.mark.parametrize("repo_type", ["models", "datasets", "spaces"])
@pytest.mark.parametrize("repo_id", ["team/demo", "gpt2"])
async def test_standard_refs_routes_and_auth(setup, repo_type, repo_id):
    app, _, calls, _ = setup
    async with ORIGINAL_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
        response = await client.get(f"/api/{repo_type}/{repo_id}/refs", headers={"authorization": "Bearer test-token"})
    assert response.status_code == 200
    assert response.json() == refs_payload()
    assert [(req.method, req.url.path) for req in calls] == [("HEAD", f"/api/{repo_type}/{repo_id}"), ("GET", f"/api/{repo_type}/{repo_id}/refs")]
    assert all(req.headers["authorization"] == "Bearer test-token" for req in calls)


@pytest.mark.asyncio
async def test_online_refresh_offline_snapshot_and_cold_miss(setup):
    app, config, calls, state = setup
    async with ORIGINAL_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
        assert (await client.get("/api/models/team/demo/refs")).json() == refs_payload(SHA1)
        state["sha"] = SHA2
        assert (await client.get("/api/models/team/demo/refs")).json() == refs_payload(SHA2)
        assert len(calls) == 4
        config.offline = True
        calls.clear()
        assert (await client.get("/api/models/team/demo/refs")).json() == refs_payload(SHA2)
        for path in ("/api/models/team/cold/refs", "/api/models/team/demo/refs?include_prs=1"):
            response = await client.get(path)
            assert response.status_code == 404
            assert response.headers["x-error-code"] == "EntryNotFound"
        assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
async def test_failed_refresh_never_replaces_good_snapshot(setup, status):
    app, config, calls, state = setup
    async with ORIGINAL_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
        assert (await client.get("/api/models/team/demo/refs")).status_code == 200
        state["status"] = status
        state["sha"] = SHA2
        assert (await client.get("/api/models/team/demo/refs")).status_code == status
        config.offline = True
        calls.clear()
        assert (await client.get("/api/models/team/demo/refs")).json() == refs_payload(SHA1)
        assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "invalid"])
async def test_invalid_or_unreachable_refs_preserve_snapshot(setup, failure):
    app, config, calls, state = setup
    async with ORIGINAL_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
        await client.get("/api/models/team/demo/refs")
        state[failure] = True
        assert (await client.get("/api/models/team/demo/refs")).status_code == 504
        config.offline = True
        calls.clear()
        assert (await client.get("/api/models/team/demo/refs")).json() == refs_payload()
        assert calls == []


@pytest.mark.asyncio
async def test_auth_cache_isolation_and_visibility_gate(setup, tmp_path):
    app, config, calls, state = setup
    auth = {"authorization": "Bearer secret-fixture-token"}
    async with ORIGINAL_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
        assert (await client.get("/api/models/team/demo/refs", headers=auth)).status_code == 200
        state["visible"] = False
        calls.clear()
        assert (await client.get("/api/models/team/demo/refs", headers=auth)).status_code == 401
        assert len(calls) == 1 and calls[0].method == "HEAD"
        config.offline = True
        calls.clear()
        assert (await client.get("/api/models/team/demo/refs", headers=auth)).status_code == 200
        for headers in ({}, {"authorization": "Bearer different-token"}):
            assert (await client.get("/api/models/team/demo/refs", headers=headers)).status_code == 404
        assert calls == []
    assert all("secret-fixture-token" not in str(path) for path in tmp_path.rglob("*"))
    assert all(b"secret-fixture-token" not in path.read_bytes() for path in tmp_path.rglob("*") if path.is_file())


@pytest.mark.asyncio
async def test_proxy_and_cache_rules_still_apply(setup, tmp_path):
    app, config, calls, _ = setup
    async with ORIGINAL_CLIENT(transport=httpx.ASGITransport(app=app), base_url="http://olah.test") as client:
        config.cache = OlahRuleList.from_list([])
        assert (await client.get("/api/models/team/demo/refs")).status_code == 200
        assert not list(tmp_path.rglob("*.json"))
        config.offline = True
        calls.clear()
        assert (await client.get("/api/models/team/demo/refs")).status_code == 404
        config.proxy = OlahRuleList.from_list([])
        assert (await client.get("/api/models/team/demo/refs")).status_code == 401
        config.offline = False
        assert (await client.get("/api/models/team/demo/refs")).status_code == 401
        assert calls == []


def build_git_refs(root):
    repo = git.Repo.init(root, initial_branch="trunk")
    actor = git.Actor("Test", "test@example.invalid")
    (root / "README.md").write_text("first\n")
    repo.index.add(["README.md"])
    first = repo.index.commit("first", author=actor, committer=actor)
    repo.create_head("feature/nested", first)
    repo.create_tag("light", first)
    with repo.config_writer() as writer:
        writer.set_value("user", "name", "Test")
        writer.set_value("user", "email", "test@example.invalid")
    repo.create_tag("annotated", first, message="annotated tag")
    (root / "README.md").write_text("second\n")
    repo.index.add(["README.md"])
    second = repo.index.commit("second", author=actor, committer=actor)
    repo.git.update_ref("refs/convert/parquet", first.hexsha)
    repo.git.update_ref("refs/pr/1", second.hexsha)
    repo.git.pack_refs("--all", "--prune")
    return repo, first, second


def test_local_real_refs_include_packed_annotated_and_optional_prs(tmp_path):
    repo, first, second = build_git_refs(tmp_path)
    mirror = LocalMirrorRepo(str(tmp_path), "models", "team", "demo")
    refs = mirror.get_refs()
    assert {item["name"]: item["targetCommit"] for item in refs["branches"]} == {"feature/nested": first.hexsha, "trunk": second.hexsha}
    assert {item["name"]: item["targetCommit"] for item in refs["tags"]} == {"annotated": first.hexsha, "light": first.hexsha}
    assert refs["converts"] == [{"name": "parquet", "ref": "refs/convert/parquet", "targetCommit": first.hexsha}]
    assert "pullRequests" not in refs
    assert mirror.get_refs(True)["pullRequests"] == [{"name": "1", "ref": "refs/pr/1", "targetCommit": second.hexsha}]
    repo.heads.trunk.set_commit(first)
    assert {r["name"]: r["targetCommit"] for r in mirror.get_refs()["branches"]}["trunk"] == first.hexsha


def test_empty_git_repo_does_not_invent_main(tmp_path):
    git.Repo.init(tmp_path)
    assert LocalMirrorRepo(str(tmp_path), "models", None, "empty").get_refs(True) == {"branches": [], "tags": [], "converts": [], "pullRequests": []}


@pytest.mark.parametrize("offline", [False, True])
def test_real_hfapi_parses_local_refs(setup, tmp_path, monkeypatch, offline):
    app, config, calls, _ = setup
    root = tmp_path / "mirrors" / "models" / "demo"
    root.mkdir(parents=True)
    _, first, second = build_git_refs(root)
    config.mirrors_path = [str(tmp_path / "mirrors")]
    config.offline = offline
    with TestClient(app) as client:
        monkeypatch.setattr("huggingface_hub.hf_api.get_session", lambda: client)
        refs = HfApi(endpoint="http://testserver", token=False).list_repo_refs("demo", include_pull_requests=True)
    assert {ref.name: ref.target_commit for ref in refs.branches} == {"feature/nested": first.hexsha, "trunk": second.hexsha}
    assert refs.tags[0].target_commit == first.hexsha
    assert refs.pull_requests[0].target_commit == second.hexsha
    assert len(calls) == (0 if offline else 1)
