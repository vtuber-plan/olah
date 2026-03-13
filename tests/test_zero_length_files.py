import asyncio
import json
from types import SimpleNamespace

from fastapi import Request

from olah.proxy import files as proxy_files
from olah.proxy.result import ProxyResult, single_chunk_body


def _make_request(method: str = "HEAD", headers=None) -> Request:
    raw_headers = []
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode("ascii"), value.encode("ascii")))
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": "/test",
        "raw_path": b"/test",
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 18090),
    }
    return Request(scope)


def _make_app(tmp_path):
    config = SimpleNamespace(
        offline=False,
        hf_netloc="huggingface.co",
        hf_lfs_netloc="cdn-lfs.huggingface.co",
        repos_path=str(tmp_path / "repos"),
        hf_url_base=lambda: "https://huggingface.co",
        hf_lfs_url_base=lambda: "https://cdn-lfs.huggingface.co",
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


def test_file_realtime_stream_handles_zero_length_head(monkeypatch, tmp_path):
    async def fake_pathsinfo_generator(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(json.dumps([{"size": 0}])),
        )

    async def fake_resource_etag(*args, **kwargs):
        return '"empty-etag"'

    async def fake_file_chunk_head(*args, **kwargs):
        if False:
            yield b""

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_resource_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_head", fake_file_chunk_head)

    async def run():
        result = await proxy_files._file_realtime_stream(
            app=_make_app(tmp_path),
            repo_type="models",
            org="nvidia",
            repo="NVIDIA-Nemotron-3-Super-120B-A12B-FP8",
            file_path="__init__.py",
            save_path=str(tmp_path / "save"),
            head_path=str(tmp_path / "head"),
            url="http://127.0.0.1:18090/nvidia/NVIDIA-Nemotron-3-Super-120B-A12B-FP8/resolve/main/__init__.py",
            request=_make_request("HEAD"),
            method="HEAD",
            allow_cache=True,
            commit="main",
        )
        body = [chunk async for chunk in result.body]
        return result.status_code, result.headers, body

    status_code, headers, body = asyncio.run(run())

    assert status_code == 200
    assert headers["content-length"] == "0"
    assert "content-range" not in headers
    assert headers["etag"] == '"empty-etag"'
    assert body == []
