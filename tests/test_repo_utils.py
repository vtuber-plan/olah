import asyncio
import gzip
import json
from types import SimpleNamespace

import httpx

from olah.utils import cache_utils, repo_utils


def _make_app(tmp_path, offline=False):
    config = SimpleNamespace(
        repos_path=str(tmp_path),
        offline=offline,
        hf_url_base=lambda: "https://huggingface.example",
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


def test_repo_path_helpers_build_expected_locations():
    repos_path = "/srv/repos"

    assert repo_utils.get_org_repo(None, "demo") == "demo"
    assert repo_utils.get_org_repo("team", "demo") == "team/demo"
    assert repo_utils.parse_org_repo("team/demo") == ("team", "demo")
    assert repo_utils.parse_org_repo("demo") == (None, "demo")
    assert repo_utils.parse_org_repo("too/many/parts") == (None, None)
    assert repo_utils.get_meta_save_dir(repos_path, "models", "team", "demo") == (
        "/srv/repos/api/models/team/demo/revision"
    )
    assert repo_utils.get_meta_save_path(repos_path, "models", "team", "demo", "main") == (
        "/srv/repos/api/models/team/demo/revision/main/meta_get.json"
    )
    assert repo_utils.get_file_save_path(
        repos_path, "models", "team", "demo", "main", "weights/model.bin"
    ) == "/srv/repos/heads/models/team/demo/resolve_head/main/weights/model.bin"


def test_get_newest_commit_hf_offline_returns_latest_revision(tmp_path):
    app = _make_app(tmp_path, offline=True)
    older_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "old"
    newer_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "new"
    older_path.mkdir(parents=True)
    newer_path.mkdir(parents=True)
    (older_path / "meta_head.json").write_text(
        json.dumps({"lastModified": "2024-01-01T00:00:00", "sha": "old-sha"}),
        encoding="utf-8",
    )
    (newer_path / "meta_head.json").write_text(
        json.dumps({"lastModified": "2024-02-01T00:00:00", "sha": "new-sha"}),
        encoding="utf-8",
    )

    commit = asyncio.run(repo_utils.get_newest_commit_hf_offline(app, "models", "team", "demo"))

    assert commit == "new-sha"


def test_get_commit_hf_offline_reads_cached_request(tmp_path):
    app = _make_app(tmp_path, offline=True)
    save_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main" / "meta_get.json"
    save_path.parent.mkdir(parents=True)
    asyncio.run(
        cache_utils.write_cache_request(
            str(save_path),
            status_code=200,
            headers={"content-type": "application/json"},
            content=json.dumps({"sha": "cached-sha"}).encode("utf-8"),
        )
    )

    commit = asyncio.run(repo_utils.get_commit_hf_offline(app, "models", "team", "demo", "main"))

    assert commit == "cached-sha"


def test_get_commit_hf_offline_reads_gzip_cached_request(tmp_path):
    app = _make_app(tmp_path, offline=True)
    save_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main" / "meta_get.json"
    save_path.parent.mkdir(parents=True)
    asyncio.run(
        cache_utils.write_cache_request(
            str(save_path),
            status_code=200,
            headers={"content-type": "application/json", "content-encoding": "gzip"},
            content=gzip.compress(json.dumps({"sha": "cached-sha"}).encode("utf-8")),
        )
    )

    commit = asyncio.run(repo_utils.get_commit_hf_offline(app, "models", "team", "demo", "main"))

    assert commit == "cached-sha"


def test_get_commit_hf_offline_decodes_gzip_with_mixed_case_header(tmp_path):
    """A capitalised Content-Encoding header must still trigger decompression."""
    app = _make_app(tmp_path, offline=True)
    save_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main" / "meta_get.json"
    save_path.parent.mkdir(parents=True)
    asyncio.run(
        cache_utils.write_cache_request(
            str(save_path),
            status_code=200,
            headers={"Content-Encoding": "gzip"},
            content=gzip.compress(json.dumps({"sha": "mixed-case-sha"}).encode("utf-8")),
        )
    )

    commit = asyncio.run(repo_utils.get_commit_hf_offline(app, "models", "team", "demo", "main"))

    assert commit == "mixed-case-sha"


def test_get_commit_hf_offline_self_heals_gzip_without_encoding_header(tmp_path):
    """A gzip body with no recorded content-encoding is recovered via magic bytes."""
    app = _make_app(tmp_path, offline=True)
    save_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main" / "meta_get.json"
    save_path.parent.mkdir(parents=True)
    asyncio.run(
        cache_utils.write_cache_request(
            str(save_path),
            status_code=200,
            headers={"content-type": "application/json"},
            content=gzip.compress(json.dumps({"sha": "magic-sha"}).encode("utf-8")),
        )
    )

    commit = asyncio.run(repo_utils.get_commit_hf_offline(app, "models", "team", "demo", "main"))

    assert commit == "magic-sha"


def test_get_commit_hf_offline_returns_none_for_corrupt_gzip_cache(tmp_path):
    """A corrupt gzip cache degrades to a cache miss instead of crashing with a 500."""
    app = _make_app(tmp_path, offline=True)
    save_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main" / "meta_get.json"
    save_path.parent.mkdir(parents=True)
    asyncio.run(
        cache_utils.write_cache_request(
            str(save_path),
            status_code=200,
            headers={"content-encoding": "gzip"},
            # Magic bytes followed by junk: declares gzip but is not decodable.
            content=b"\x1f\x8b" + b"not-really-gzip",
        )
    )

    commit = asyncio.run(repo_utils.get_commit_hf_offline(app, "models", "team", "demo", "main"))

    assert commit is None


def test_get_newest_commit_hf_offline_reads_proxy_cache_envelope(tmp_path):
    """A proxy-cache envelope (status_code/headers/content) meta_head.json is unwrapped."""
    app = _make_app(tmp_path, offline=True)
    revision_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main"
    revision_path.mkdir(parents=True)
    payload = json.dumps({"sha": "envelope-sha", "lastModified": "2024-04-01T00:00:00"}).encode("utf-8")
    asyncio.run(
        cache_utils.write_cache_request(
            str(revision_path / "meta_head.json"),
            status_code=200,
            headers={"content-type": "application/json"},
            content=payload,
        )
    )

    commit = asyncio.run(repo_utils.get_newest_commit_hf_offline(app, "models", "team", "demo"))

    assert commit == "envelope-sha"


def test_get_newest_commit_hf_offline_skips_empty_head_cache(tmp_path):
    """An empty proxy HEAD cache (no body) is skipped rather than crashing on a missing key."""
    app = _make_app(tmp_path, offline=True)
    revision_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "main"
    revision_path.mkdir(parents=True)
    asyncio.run(
        cache_utils.write_cache_request(
            str(revision_path / "meta_head.json"),
            status_code=200,
            headers={"content-type": "application/json"},
            content=b"",
        )
    )

    commit = asyncio.run(repo_utils.get_newest_commit_hf_offline(app, "models", "team", "demo"))

    assert commit is None


def test_get_newest_commit_hf_falls_back_to_offline_on_timeout(monkeypatch, tmp_path):
    app = _make_app(tmp_path, offline=False)
    revision_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "cached"
    revision_path.mkdir(parents=True)
    (revision_path / "meta_head.json").write_text(
        json.dumps({"lastModified": "2024-03-01T00:00:00", "sha": "offline-sha"}),
        encoding="utf-8",
    )

    class FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            raise httpx.TimeoutException("timeout")

    monkeypatch.setattr(repo_utils.httpx, "AsyncClient", FakeAsyncClient)

    commit = asyncio.run(
        repo_utils.get_newest_commit_hf(app, "models", "team", "demo", authorization="Bearer token")
    )

    assert commit == "offline-sha"


def test_get_newest_commit_hf_falls_back_to_offline_on_connect_error(monkeypatch, tmp_path):
    app = _make_app(tmp_path, offline=False)
    revision_path = tmp_path / "api" / "models" / "team" / "demo" / "revision" / "cached"
    revision_path.mkdir(parents=True)
    (revision_path / "meta_head.json").write_text(
        json.dumps({"lastModified": "2024-03-01T00:00:00", "sha": "offline-sha"}),
        encoding="utf-8",
    )

    class FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *args, **kwargs):
            raise httpx.ConnectError("offline")

    monkeypatch.setattr(repo_utils.httpx, "AsyncClient", FakeAsyncClient)

    commit = asyncio.run(
        repo_utils.get_newest_commit_hf(app, "models", "team", "demo", authorization="Bearer token")
    )

    assert commit == "offline-sha"


def test_check_commit_hf_returns_false_when_upstream_request_fails(monkeypatch, tmp_path):
    app = _make_app(tmp_path, offline=False)

    class FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, *args, **kwargs):
            raise httpx.ConnectError("offline")

    monkeypatch.setattr(repo_utils.httpx, "AsyncClient", FakeAsyncClient)

    ok = asyncio.run(repo_utils.check_commit_hf(app, "models", "team", "demo", commit="main"))

    assert ok is False
