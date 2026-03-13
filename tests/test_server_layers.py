import sys
import types
from types import SimpleNamespace

import pytest

if "olah.mirror.repos" not in sys.modules:
    mirror_repos_stub = types.ModuleType("olah.mirror.repos")

    class _StubLocalMirrorRepo:
        pass

    mirror_repos_stub.LocalMirrorRepo = _StubLocalMirrorRepo
    sys.modules["olah.mirror.repos"] = mirror_repos_stub

if "git" not in sys.modules:
    git_stub = types.ModuleType("git")
    git_stub.exc = SimpleNamespace(InvalidGitRepositoryError=RuntimeError)
    sys.modules["git"] = git_stub

if "portalocker" not in sys.modules:
    portalocker_stub = types.ModuleType("portalocker")

    class _Lock:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    portalocker_stub.Lock = _Lock
    portalocker_stub.LOCK_EX = 1
    portalocker_stub.LOCK_SH = 2
    sys.modules["portalocker"] = portalocker_stub

if "fastapi_utils.tasks" not in sys.modules:
    fastapi_utils_module = types.ModuleType("fastapi_utils")
    fastapi_utils_tasks = types.ModuleType("fastapi_utils.tasks")

    def repeat_every(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

    fastapi_utils_tasks.repeat_every = repeat_every
    sys.modules["fastapi_utils"] = fastapi_utils_module
    sys.modules["fastapi_utils.tasks"] = fastapi_utils_tasks

from olah import server_access, server_mirror, server_responses, server_upstream


def _make_app(*, offline=False, mirrors_path=None):
    config = SimpleNamespace(
        offline=offline,
        mirrors_path=mirrors_path or [],
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


@pytest.mark.asyncio
async def test_ensure_repo_access_validates_repo_type_and_rules(monkeypatch):
    repo = server_access.build_repo_ref("models", "team", "demo")

    async def fake_check_proxy_rules_hf(*args, **kwargs):
        return False

    monkeypatch.setattr(server_access, "check_proxy_rules_hf", fake_check_proxy_rules_hf)

    denied = await server_access.ensure_repo_access(_make_app(), repo)
    assert denied.status_code == 401

    invalid = await server_access.ensure_repo_access(
        _make_app(),
        server_access.build_repo_ref("unknown", "team", "demo"),
    )
    assert invalid.status_code == 404


def test_parse_repo_helpers_cover_compact_and_default_model_routes():
    parsed = server_access.parse_repo_ref("datasets", "team/demo")
    assert parsed == server_access.RepoRef(repo_type="datasets", org="team", repo="demo")

    default_model = server_access.parse_resolve_repo_ref("team", "demo")
    assert default_model == server_access.RepoRef(repo_type="models", org="team", repo="demo")

    assert server_access.parse_repo_ref("models", "too/many/parts") is None


def test_load_local_mirror_payload_uses_shared_lookup_and_org_optional(monkeypatch):
    seen = {}

    class FakeRepo:
        def __init__(self, git_path, repo_type, org, repo):
            seen["init"] = (git_path, repo_type, org, repo)

    monkeypatch.setattr(server_mirror, "LocalMirrorRepo", FakeRepo)
    monkeypatch.setattr(server_mirror.os.path, "exists", lambda path: path.endswith("/models/demo"))

    logger = SimpleNamespace(warning=lambda *_args, **_kwargs: None)
    payload = server_mirror.load_local_mirror_payload(
        _make_app(mirrors_path=["/mirror-root"]),
        server_access.build_repo_ref("models", None, "demo"),
        lambda local_repo: {"repo": local_repo.__class__.__name__},
        logger,
    )

    assert payload == {"repo": "FakeRepo"}
    assert seen["init"] == ("/mirror-root/models/demo", "models", None, "demo")


@pytest.mark.asyncio
async def test_resolve_requested_commit_centralizes_repo_and_revision_errors(monkeypatch):
    app = _make_app()
    repo = server_access.build_repo_ref("models", "team", "demo")

    async def fake_check_commit_hf(app, repo_type, org, repo_name, commit, authorization=None):
        return commit is None

    monkeypatch.setattr(server_upstream, "check_commit_hf", fake_check_commit_hf)
    monkeypatch.setattr(server_upstream, "get_commit_hf", pytest.fail)

    _, revision_error = await server_upstream.resolve_requested_commit(
        app,
        repo,
        "missing",
        "Bearer t",
        missing_commit_response="revision_not_found",
    )
    assert revision_error.status_code == 404

    _, repo_error = await server_upstream.resolve_requested_commit(
        app,
        repo,
        "missing",
        "Bearer t",
        missing_commit_response="repo_not_found",
    )
    assert repo_error.status_code == 401


@pytest.mark.asyncio
async def test_prepare_revision_generator_refreshes_alias_before_resolved_commit():
    app = _make_app(offline=False)
    calls = []

    async def generator_factory(commit, override_cache):
        calls.append((commit, override_cache))
        yield 200
        yield {"x-commit": commit}
        yield commit.encode("utf-8")

    generator = await server_upstream.prepare_revision_generator(
        app,
        server_upstream.ResolvedCommit(requested="main", resolved="abc123"),
        generator_factory,
    )

    assert await generator.__anext__() == 200
    assert calls == [("main", True), ("abc123", True)]
    assert await generator.__anext__() == {"x-commit": "abc123"}


@pytest.mark.asyncio
async def test_build_streaming_response_supports_generators_with_and_without_status():
    async def generator_with_status():
        yield 206
        yield {"content-range": "bytes 0-1/2"}
        yield b"ab"

    response = await server_responses.build_streaming_response(generator_with_status(), include_status_code=True)
    assert response.status_code == 206
    assert response.headers["content-range"] == "bytes 0-1/2"

    async def generator_without_status():
        yield {"content-type": "application/json"}
        yield b"{}"

    response = await server_responses.build_streaming_response(generator_without_status(), include_status_code=False)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"


def test_importing_server_module_has_no_init_side_effects():
    import importlib

    sys.modules.pop("olah.server", None)
    server_module = importlib.import_module("olah.server")

    assert not hasattr(server_module.app.state, "app_settings")


def test_main_and_cli_share_run_server(monkeypatch):
    import importlib

    server_module = importlib.import_module("olah.server")
    calls = []
    args = SimpleNamespace(host="127.0.0.1", port=8090, ssl_key=None, ssl_cert=None)

    monkeypatch.setattr(server_module, "init", lambda: calls.append("init") or args)
    monkeypatch.setattr(server_module, "run_server", lambda received: calls.append(("run_server", received)))

    server_module.main()
    server_module.cli()

    assert calls == ["init", ("run_server", args), "init", ("run_server", args)]
