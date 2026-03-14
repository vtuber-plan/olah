import json
from types import SimpleNamespace

import pytest

from olah.proxy import pathsinfo
from olah.proxy.result import single_chunk_body


def _make_app(tmp_path):
    config = SimpleNamespace(
        repos_path=str(tmp_path / "repos"),
        hf_url_base=lambda: "https://huggingface.example",
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


@pytest.mark.asyncio
async def test_pathsinfo_proxy_strips_content_length_and_writes_cache(monkeypatch, tmp_path):
    captured = {}

    class FakeResponse:
        status_code = 200
        headers = {"content-type": "application/json", "x-test": "1"}
        content = b'[{"size": 10}]'

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            captured["init"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, **kwargs):
            captured["request"] = kwargs
            return FakeResponse()

    saved = {}

    async def fake_write_cache_request(save_path, status_code, headers, content):
        saved["save_path"] = save_path
        saved["status_code"] = status_code
        saved["headers"] = dict(headers)
        saved["content"] = content

    monkeypatch.setattr(pathsinfo.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(pathsinfo, "write_cache_request", fake_write_cache_request)

    result = await pathsinfo._pathsinfo_proxy(
        app=_make_app(tmp_path),
        headers={"authorization": "Bearer t", "content-length": "999"},
        pathsinfo_url="https://huggingface.example/api/models/team/demo/paths-info/main",
        method="post",
        path="config.json",
        allow_cache=True,
        save_path=str(tmp_path / "repos" / "cache.json"),
    )
    assert result == (200, FakeResponse.headers, FakeResponse.content)
    assert "content-length" not in captured["request"]["headers"]
    assert captured["request"]["data"] == {"paths": "config.json"}
    assert saved["status_code"] == 200
    assert saved["content"] == b'[{"size": 10}]'


@pytest.mark.asyncio
async def test_pathsinfo_generator_prefers_cache_and_aggregates_valid_list_payloads(monkeypatch, tmp_path):
    app = _make_app(tmp_path)
    cache_root = tmp_path / "repos" / "api" / "models" / "team" / "demo" / "paths-info" / "main"
    (cache_root / "a.txt").mkdir(parents=True)
    ((cache_root / "a.txt") / "paths-info_post.json").write_text("cached", encoding="utf-8")
    captured = []

    async def fake_check_cache_rules_hf(*args, **kwargs):
        return True

    async def fake_pathsinfo_cache(save_path):
        return 200, {"content-type": "application/json"}, b'[{"path": "a.txt", "size": 1}]'

    async def fake_pathsinfo_proxy(*args, **kwargs):
        captured.append((args, kwargs))
        return 200, {"content-type": "application/json"}, b'[{"path": "b.txt", "size": 2}]'

    monkeypatch.setattr(pathsinfo, "check_cache_rules_hf", fake_check_cache_rules_hf)
    monkeypatch.setattr(pathsinfo, "_pathsinfo_cache", fake_pathsinfo_cache)
    monkeypatch.setattr(pathsinfo, "_pathsinfo_proxy", fake_pathsinfo_proxy)

    result = await pathsinfo.pathsinfo_generator(
        app=app,
        repo_type="models",
        org="team",
        repo="demo",
        commit="main",
        paths=["a.txt", "b.txt"],
        override_cache=False,
        method="post",
        authorization="Bearer t",
    )
    body = [chunk async for chunk in result.body]
    content = json.loads(body[0])

    assert result.status_code == 200
    assert result.headers == {"content-type": "application/json"}
    assert content == [{"path": "a.txt", "size": 1}, {"path": "b.txt", "size": 2}]
    assert captured[0][0][1] == {"authorization": "Bearer t"}


@pytest.mark.asyncio
async def test_pathsinfo_generator_propagates_first_non_200_response(monkeypatch, tmp_path):
    app = _make_app(tmp_path)

    async def fake_check_cache_rules_hf(*args, **kwargs):
        return False

    async def fake_pathsinfo_proxy(*args, **kwargs):
        return 500, {"x-error": "upstream"}, b"boom"

    monkeypatch.setattr(pathsinfo, "check_cache_rules_hf", fake_check_cache_rules_hf)
    monkeypatch.setattr(pathsinfo, "_pathsinfo_proxy", fake_pathsinfo_proxy)

    result = await pathsinfo.pathsinfo_generator(
        app=app,
        repo_type="models",
        org="team",
        repo="demo",
        commit="main",
        paths=["a", "b", "c", "d"],
        override_cache=True,
        method="post",
        authorization=None,
    )
    body = [chunk async for chunk in result.body]
    assert result.status_code == 500
    assert result.headers == {"x-error": "upstream"}
    assert body == [b"boom"]


@pytest.mark.asyncio
async def test_pathsinfo_generator_rejects_invalid_json_and_non_list_payloads(monkeypatch, tmp_path):
    app = _make_app(tmp_path)

    async def fake_check_cache_rules_hf(*args, **kwargs):
        return False

    responses = iter(
        [
            (200, {"content-type": "application/json"}, b"not-json"),
            (200, {"content-type": "application/json"}, b'{"not": "a list"}'),
        ]
    )

    async def fake_pathsinfo_proxy(*args, **kwargs):
        return next(responses)

    monkeypatch.setattr(pathsinfo, "check_cache_rules_hf", fake_check_cache_rules_hf)
    monkeypatch.setattr(pathsinfo, "_pathsinfo_proxy", fake_pathsinfo_proxy)

    invalid_json = await pathsinfo.pathsinfo_generator(
        app=app,
        repo_type="models",
        org="team",
        repo="demo",
        commit="main",
        paths=["a"],
        override_cache=True,
        method="post",
        authorization=None,
    )
    invalid_json_body = [chunk async for chunk in invalid_json.body]
    assert invalid_json.status_code == 504
    assert invalid_json.headers["x-error-code"] == "ProxyInvalidData"
    assert invalid_json_body == [b""]

    invalid_list = await pathsinfo.pathsinfo_generator(
        app=app,
        repo_type="models",
        org="team",
        repo="demo",
        commit="main",
        paths=["b"],
        override_cache=True,
        method="post",
        authorization=None,
    )
    invalid_list_body = [chunk async for chunk in invalid_list.body]
    assert invalid_list.status_code == 504
    assert invalid_list.headers["x-error-code"] == "ProxyInvalidData"
    assert invalid_list_body == [b""]
