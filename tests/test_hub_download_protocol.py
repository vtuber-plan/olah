"""Exercise the Hub client's refs -> immutable tree -> HEAD -> download chain.

The small byte fixtures are not inference models. Upstream traffic is mocked;
requests still traverse Olah's real routers, cache and local Git mirror layers.
"""
import hashlib
import json
from types import SimpleNamespace

import git
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from huggingface_hub import HfApi

from olah.configs import OlahConfig
from olah.server_routes import router

HTTP_CLIENT = httpx.AsyncClient
REPO = "team/tiny"
API = f"/api/models/{REPO}"
OLD = b"old tiny fixture\n"
NEW = b"new tiny fixture\n"
SHA1 = "1" * 40
SHA2 = "2" * 40


class RawBytes(httpx.AsyncByteStream):
    def __init__(self, data):
        self.data = data

    async def __aiter__(self):
        yield self.data


@pytest.mark.parametrize("mode", ["proxy_cache", "local_git"])
def test_hfapi_discovery_and_immutable_download_online_then_offline(tmp_path, monkeypatch, mode):
    config = OlahConfig()
    config.repos_path = str(tmp_path / "cache")
    config.hf_netloc = "upstream.invalid"
    config.cache_block_size = 8
    config.cache_chunk_size = 4
    app = FastAPI()
    app.state.app_settings = SimpleNamespace(config=config)
    app.include_router(router)
    calls = []
    state = {"sha": SHA1}
    contents = {SHA1: OLD, SHA2: NEW}

    if mode == "local_git":
        root = tmp_path / "mirror" / "models" / REPO
        root.mkdir(parents=True)
        repo = git.Repo.init(root, initial_branch="main")
        actor = git.Actor("Fixture", "fixture@example.invalid")

        def commit(data):
            (root / "tiny.gguf").write_bytes(data)
            repo.index.add(["tiny.gguf"])
            return repo.index.commit("tiny protocol fixture", author=actor, committer=actor).hexsha

        state["sha"] = commit(OLD)
        config.mirrors_path = [str(tmp_path / "mirror")]

    async def upstream(request):
        calls.append((request.method, request.url.path, request.headers.get("range")))
        path = request.url.path
        assert request.url.host == "upstream.invalid"
        if path.endswith("/refs"):
            payload = {"branches": [{"name": "main", "ref": "refs/heads/main", "targetCommit": state["sha"]}], "tags": [], "converts": []}
            data = json.dumps(payload).encode()
        elif "/tree/" in path or "/paths-info/" in path:
            kind = "tree" if "/tree/" in path else "paths-info"
            sha = path.split(f"/{kind}/", 1)[1].split("/", 1)[0]
            content = contents[sha]
            data = json.dumps([{"type": "file", "oid": hashlib.sha1(content).hexdigest(), "path": "tiny.gguf", "size": len(content)}]).encode()
        elif "/resolve/" in path:
            sha = path.split("/resolve/", 1)[1].split("/", 1)[0]
            content = contents[sha]
            headers = {
                "etag": f'"{hashlib.sha1(content).hexdigest()}"',
                "content-length": str(len(content)),
                "x-repo-commit": sha,
            }
            if request.method == "HEAD":
                return httpx.Response(200, headers=headers, stream=RawBytes(b""))
            data = content
            if request.headers.get("range"):
                start, end = request.headers["range"].removeprefix("bytes=").split("-")
                data = content[int(start):int(end) + 1]
                headers["content-range"] = f"bytes {start}-{end}/{len(content)}"
                headers["content-length"] = str(len(data))
            return httpx.Response(206 if "content-range" in headers else 200, headers=headers, stream=RawBytes(data))
        else:
            assert request.method == "HEAD", "Immutable SHA lookup must not fetch revision metadata"
            data = b""
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=RawBytes(data))

    class UpstreamClient(HTTP_CLIENT):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", httpx.MockTransport(upstream))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", UpstreamClient)
    with TestClient(app) as client:
        monkeypatch.setattr("huggingface_hub.hf_api.get_session", lambda: client)
        # huggingface_hub >= 2.0 moved the shared session from utils._pagination
        # (its own binding, pre-2.0) to utils._http, where http_backoff resolves
        # it at call time. Patch the binding that paginate() actually uses.
        try:
            monkeypatch.setattr("huggingface_hub.utils._pagination.get_session", lambda: client)
        except AttributeError:
            monkeypatch.setattr("huggingface_hub.utils._http.get_session", lambda: client)
        api = HfApi(endpoint="http://testserver", token=False)

        def discover(expected):
            sha = next(ref.target_commit for ref in api.list_repo_refs(REPO).branches if ref.name == "main")
            tree = list(api.list_repo_tree(REPO, revision=sha, recursive=True))
            assert [(file.path, file.size) for file in tree] == [("tiny.gguf", len(expected))]
            return sha

        def download(sha, expected):
            url = f"/{REPO}/resolve/{sha}/tiny.gguf"
            head = client.head(url)
            assert head.status_code == 200
            assert head.headers["x-repo-commit"] == sha
            assert int(head.headers["content-length"]) == len(expected)
            assert client.get(url).content == expected
            ranged = client.get(url, headers={"range": "bytes=4-7"})
            if mode == "proxy_cache":
                assert ranged.status_code == 206
                assert ranged.headers["content-range"] == f"bytes 4-7/{len(expected)}"
                assert ranged.content == expected[4:8]
            else:
                # Existing Git mirror behavior: HTTP permits ignoring Range and
                # returning the full representation. Do not claim partial support.
                assert ranged.status_code == 200
                assert ranged.content == expected

        old_sha = discover(OLD)
        old_blob = list(api.list_repo_tree(REPO, revision=old_sha, recursive=True))[0].blob_id
        download(old_sha, OLD)
        state["sha"] = commit(NEW) if mode == "local_git" else SHA2
        new_sha = discover(NEW)
        assert old_sha != new_sha
        assert list(api.list_repo_tree(REPO, revision=new_sha, recursive=True))[0].blob_id != old_blob
        download(new_sha, NEW)
        # A moving main must not change a tree/file already addressed by old SHA.
        assert list(api.list_repo_tree(REPO, revision=old_sha, recursive=True))[0].blob_id == old_blob
        download(old_sha, OLD)

        config.offline = True
        calls.clear()
        assert discover(NEW) == new_sha
        download(new_sha, NEW)
        download(old_sha, OLD)
        assert list(api.list_repo_tree(REPO, revision=old_sha, recursive=True))[0].blob_id == old_blob
        assert calls == []
    if mode == "local_git":
        repo.close()
