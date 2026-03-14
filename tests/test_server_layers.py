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

from olah import errors, server_access, server_mirror, server_responses, server_upstream
from olah import server_api_routes, server_file_routes
from olah.utils import logging as olah_logging
from olah.proxy.result import ProxyResult, single_chunk_body


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


@pytest.mark.asyncio
async def test_ensure_repo_visibility_checks_upstream_visibility_with_auth(monkeypatch):
    repo = server_access.build_repo_ref("models", "team", "demo")
    captured = {}

    async def fake_check_proxy_rules_hf(*args, **kwargs):
        return True

    async def fake_check_commit_hf(app, repo_type, org, repo_name, commit, authorization=None):
        captured["call"] = (repo_type, org, repo_name, commit, authorization)
        return False

    monkeypatch.setattr(server_access, "check_proxy_rules_hf", fake_check_proxy_rules_hf)
    monkeypatch.setattr(server_access, "check_commit_hf", fake_check_commit_hf)

    denied = await server_access.ensure_repo_visibility(_make_app(), repo, "Bearer secret")

    assert denied.status_code == 401
    assert captured["call"] == ("models", "team", "demo", None, "Bearer secret")


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
async def test_resolve_requested_commit_skips_duplicate_repo_check_after_visibility_pass(monkeypatch):
    app = _make_app()
    repo = server_access.build_repo_ref("models", "team", "demo")
    calls = []

    async def fake_check_commit_hf(app, repo_type, org, repo_name, commit, authorization=None):
        calls.append(commit)
        return True

    async def fake_get_commit_hf(*args, **kwargs):
        return "abc123"

    monkeypatch.setattr(server_upstream, "check_commit_hf", fake_check_commit_hf)
    monkeypatch.setattr(server_upstream, "get_commit_hf", fake_get_commit_hf)

    resolved, error = await server_upstream.resolve_requested_commit(
        app,
        repo,
        "main",
        "Bearer t",
        repo_visible=True,
    )

    assert error is None
    assert resolved == server_upstream.ResolvedCommit(requested="main", resolved="abc123")
    assert calls == ["main"]


@pytest.mark.asyncio
async def test_prepare_revision_generator_refreshes_alias_before_resolved_commit():
    app = _make_app(offline=False)
    calls = []

    async def generator_factory(commit, override_cache):
        calls.append((commit, override_cache))
        return ProxyResult(
            status_code=200,
            headers={"x-commit": commit},
            body=single_chunk_body(commit.encode("utf-8")),
        )

    generator = await server_upstream.prepare_revision_generator(
        app,
        server_upstream.ResolvedCommit(requested="main", resolved="abc123"),
        generator_factory,
    )

    assert generator.status_code == 200
    assert calls == [("main", True), ("abc123", True)]
    assert generator.headers == {"x-commit": "abc123"}


@pytest.mark.asyncio
async def test_build_streaming_response_supports_generators_with_and_without_status():
    response = await server_responses.build_streaming_response(
        ProxyResult(
            status_code=206,
            headers={"content-range": "bytes 0-1/2"},
            body=single_chunk_body(b"ab"),
        )
    )
    assert response.status_code == 206
    assert response.headers["content-range"] == "bytes 0-1/2"

    response = await server_responses.build_streaming_response(
        ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(b"{}"),
        )
    )
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


def test_build_logger_redirects_stdout_and_stderr_only_once(tmp_path):
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    original_handler = olah_logging.handler

    try:
        olah_logging.handler = None
        olah_logging._original_stdout = original_stdout
        olah_logging._original_stderr = original_stderr

        first = olah_logging.build_logger("olah-test-1", "test.log", logger_dir=str(tmp_path))
        stdout_wrapper = sys.stdout
        stderr_wrapper = sys.stderr

        second = olah_logging.build_logger("olah-test-2", "test.log", logger_dir=str(tmp_path))

        assert first.name == "olah-test-1"
        assert second.name == "olah-test-2"
        assert sys.stdout is stdout_wrapper
        assert sys.stderr is stderr_wrapper
        assert stdout_wrapper.terminal is original_stdout
        assert stderr_wrapper.terminal is original_stderr
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        if olah_logging.handler is not None:
            olah_logging.handler.close()
        olah_logging.handler = original_handler


@pytest.mark.asyncio
async def test_meta_proxy_common_checks_visibility_before_local_mirror(monkeypatch):
    app = _make_app()
    app.state.logger = None

    async def fake_ensure_repo_visibility(app, repo, authorization):
        return errors.error_repo_not_found()

    monkeypatch.setattr(server_api_routes, "ensure_repo_visibility", fake_ensure_repo_visibility)
    monkeypatch.setattr(server_api_routes, "load_local_mirror_payload", pytest.fail)

    response = await server_api_routes.meta_proxy_common(
        app,
        repo_type="models",
        org="team",
        repo="demo",
        commit="main",
        method="get",
        authorization=None,
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_file_get_common_checks_visibility_before_local_mirror(monkeypatch):
    from fastapi import Request

    app = _make_app()
    app.state.logger = None

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/team/demo/resolve/main/file.bin",
        "raw_path": b"/team/demo/resolve/main/file.bin",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 18090),
    }

    async def fake_ensure_repo_visibility(app, repo, authorization):
        return errors.error_repo_not_found()

    monkeypatch.setattr(server_file_routes, "ensure_repo_visibility", fake_ensure_repo_visibility)
    monkeypatch.setattr(server_file_routes, "load_local_mirror_payload", pytest.fail)

    response = await server_file_routes.file_get_common(
        app,
        repo_type="models",
        org="team",
        repo="demo",
        commit="main",
        file_path="file.bin",
        request=Request(scope),
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_cdn_proxy_common_checks_visibility_before_generator(monkeypatch):
    from fastapi import Request

    app = _make_app()
    app.state.logger = None

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/team/demo/hash.bin",
        "raw_path": b"/team/demo/hash.bin",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 18090),
    }

    async def fake_ensure_repo_visibility(app, repo, authorization):
        return errors.error_repo_not_found()

    monkeypatch.setattr(server_file_routes, "ensure_repo_visibility", fake_ensure_repo_visibility)
    monkeypatch.setattr(server_file_routes, "cdn_file_get_generator", pytest.fail)

    response = await server_file_routes.cdn_proxy_common(
        app,
        repo_type="models",
        org="team",
        repo="demo",
        hash_file="hash.bin",
        request=Request(scope),
        method="GET",
    )

    assert response.status_code == 401
