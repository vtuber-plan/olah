import json
from types import SimpleNamespace

import pytest
from git import Actor, Repo

from olah import errors, server_api_routes, server_upstream
from olah.mirror.repos import LocalMirrorRepo
from olah.proxy.result import ProxyResult, single_chunk_body


@pytest.fixture
def local_tree_repo(tmp_path):
    repo_dir = tmp_path / "models" / "team" / "demo"
    repo_dir.mkdir(parents=True)
    repo = Repo.init(repo_dir, initial_branch="main")
    actor = Actor("Test User", "test@example.com")

    def commit(files, message, timestamp):
        for path, content in files.items():
            target = repo_dir / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        repo.index.add(list(files))
        return repo.index.commit(
            message,
            author=actor,
            committer=actor,
            author_date=timestamp,
            commit_date=timestamp,
        )

    first = commit(
        {
            "README.md": "first\n",
            "weights/model.bin": "old weights\n",
            "weights/nested/config.json": "{}\n",
        },
        "first commit",
        "2024-01-01 00:00:00 +0000",
    )
    repo.create_head("stable", first)
    repo.create_tag("v1", first)
    second = commit(
        {
            "README.md": "second\n",
            "new.txt": "new\n",
            "weights/model.bin": "new weights\n",
        },
        "second commit",
        "2024-02-01 00:00:00 +0000",
    )
    mirror = LocalMirrorRepo(str(repo_dir), "models", "team", "demo")
    yield SimpleNamespace(
        mirror=mirror,
        repo=repo,
        root=tmp_path,
        first=first,
        second=second,
    )
    mirror._git_repo.close()
    repo.close()


def _paths(items):
    return {item["path"] for item in items}


@pytest.mark.parametrize("path", ["", "/", "///"])
@pytest.mark.parametrize("revision", ["main", "stable", "v1", "sha"])
def test_get_tree_lists_requested_revision_root(local_tree_repo, path, revision):
    local = local_tree_repo
    revision = local.first.hexsha if revision == "sha" else revision
    commit = local.repo.commit(revision)

    items = local.mirror.get_tree(revision, path)

    assert _paths(items) == {entry.path for entry in commit.tree}
    assert {item["path"]: item["oid"] for item in items} == {
        entry.path: entry.hexsha for entry in commit.tree
    }
    assert all("name" not in item for item in items)
    assert next(item for item in items if item["path"] == "weights")["type"] == "directory"
    assert ("new.txt" in _paths(items)) is (revision == "main")


@pytest.mark.parametrize("revision", ["main", "v1"])
@pytest.mark.parametrize("path", ["weights", "/weights/", "//weights//"])
@pytest.mark.parametrize("recursive", [False, True])
def test_get_tree_lists_subdirectory(local_tree_repo, revision, path, recursive):
    local = local_tree_repo

    items = local.mirror.get_tree(revision, path, recursive=recursive)

    expected = {"weights/model.bin", "weights/nested"}
    if recursive:
        expected.add("weights/nested/config.json")
    assert _paths(items) == expected
    assert next(item for item in items if item["path"] == "weights/model.bin")["oid"] == (
        local.repo.commit(revision).tree["weights/model.bin"].hexsha
    )


def test_get_tree_recursive_root_includes_nested_entries(local_tree_repo):
    items = local_tree_repo.mirror.get_tree("v1", "", recursive=True)

    assert _paths(items) == {
        "README.md",
        "weights",
        "weights/model.bin",
        "weights/nested",
        "weights/nested/config.json",
    }


def test_get_tree_root_preserves_expand_flag(local_tree_repo):
    plain = local_tree_repo.mirror.get_tree("main", "")
    expanded = local_tree_repo.mirror.get_tree("main", "", expand=True)

    assert all("lastCommit" not in item and "security" not in item for item in plain)
    assert _paths(plain) == _paths(expanded)
    assert all("lastCommit" in item and "security" in item for item in expanded)


@pytest.mark.parametrize(
    "revision,path",
    [
        ("missing", ""),
        ("missing", "weights"),
        ("f" * 40, ""),
        ("f" * 40, "weights"),
        ("", ""),
        ("main", "missing"),
        ("main", "weights/missing"),
        ("main", "README.md"),
        ("main", "weights/model.bin"),
        ("main", "README.md/child"),
    ],
)
def test_get_tree_unavailable_target_returns_fallback(local_tree_repo, revision, path):
    assert local_tree_repo.mirror.get_tree(revision, path) is None


def test_get_tree_empty_commit_is_an_empty_listing(tmp_path):
    repo = Repo.init(tmp_path, initial_branch="main")
    actor = Actor("Test User", "test@example.com")
    repo.index.commit("empty", author=actor, committer=actor)
    mirror = LocalMirrorRepo(str(tmp_path), "models", "team", "empty")
    try:
        assert mirror.get_tree("main", "") == []
    finally:
        mirror._git_repo.close()
        repo.close()


def _app(local):
    config = SimpleNamespace(mirrors_path=[str(local.root)], offline=False)
    return SimpleNamespace(
        state=SimpleNamespace(app_settings=SimpleNamespace(config=config))
    )


async def _visible(*args, **kwargs):
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["", "/"])
async def test_tree_api_serves_local_root_without_upstream(
    local_tree_repo, monkeypatch, path
):
    monkeypatch.setattr(server_api_routes, "ensure_repo_visibility", _visible)
    monkeypatch.setattr(server_api_routes, "resolve_requested_commit", pytest.fail)
    monkeypatch.setattr(server_api_routes, "tree_generator", pytest.fail)

    response = await server_api_routes.tree_proxy_common(
        _app(local_tree_repo),
        "models",
        "team",
        "demo",
        "v1",
        path,
        recursive=True,
        expand=False,
        method="GET",
        authorization=None,
    )

    assert response.status_code == 200
    assert _paths(json.loads(response.body)) == _paths(
        local_tree_repo.mirror.get_tree("v1", "", recursive=True)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["missing", "README.md", "weights/model.bin"])
async def test_tree_api_unlistable_local_path_uses_upstream_error(
    local_tree_repo, monkeypatch, path
):
    calls = []
    monkeypatch.setattr(server_api_routes, "ensure_repo_visibility", _visible)

    async def resolve(app, repo, commit, authorization, **kwargs):
        return server_upstream.ResolvedCommit(requested=commit, resolved=commit), None

    async def tree_generator(**kwargs):
        calls.append(kwargs["path"])
        return ProxyResult(
            status_code=404,
            headers={"x-error-code": "EntryNotFound"},
            body=single_chunk_body(b""),
        )

    monkeypatch.setattr(server_api_routes, "resolve_requested_commit", resolve)
    monkeypatch.setattr(server_api_routes, "tree_generator", tree_generator)

    response = await server_api_routes.tree_proxy_common(
        _app(local_tree_repo),
        "models",
        "team",
        "demo",
        local_tree_repo.first.hexsha,
        path,
        recursive=False,
        expand=False,
        method="GET",
        authorization=None,
    )

    assert response.status_code == 404
    assert response.headers["x-error-code"] == "EntryNotFound"
    assert calls == [path]


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", ["missing", "f" * 40])
async def test_tree_api_missing_local_revision_uses_revision_error(
    local_tree_repo, monkeypatch, revision
):
    monkeypatch.setattr(server_api_routes, "ensure_repo_visibility", _visible)
    monkeypatch.setattr(server_api_routes, "tree_generator", pytest.fail)

    async def resolve(app, repo, commit, authorization, **kwargs):
        assert commit == revision
        assert kwargs["missing_commit_response"] == "revision_not_found"
        return None, errors.error_revision_not_found(commit)

    monkeypatch.setattr(server_api_routes, "resolve_requested_commit", resolve)

    response = await server_api_routes.tree_proxy_common(
        _app(local_tree_repo),
        "models",
        "team",
        "demo",
        revision,
        "",
        recursive=False,
        expand=False,
        method="GET",
        authorization=None,
    )

    assert response.status_code == 404
    assert response.headers["x-error-code"] == "RevisionNotFound"
