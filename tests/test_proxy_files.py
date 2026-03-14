import importlib
import json
import sys
import types
from types import SimpleNamespace

import brotli
import pytest
from fastapi import Request
from olah.proxy.result import ProxyResult, single_chunk_body


def _load_proxy_files_module():
    sys.modules.pop("olah.proxy.files", None)
    if "portalocker" not in sys.modules:
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

    return importlib.import_module("olah.proxy.files")


proxy_files = _load_proxy_files_module()


def _make_request(method="GET", headers=None, url_path="/demo/file.bin") -> Request:
    raw_headers = []
    for key, value in (headers or {}).items():
        raw_headers.append((key.lower().encode("ascii"), value.encode("ascii")))
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": url_path,
        "raw_path": url_path.encode("ascii"),
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 18090),
    }
    return Request(scope)


def _make_app(tmp_path, offline=False):
    config = SimpleNamespace(
        offline=offline,
        hf_netloc="huggingface.co",
        hf_lfs_netloc="cdn-lfs.huggingface.co",
        repos_path=str(tmp_path / "repos"),
        hf_url_base=lambda: "https://huggingface.co",
        hf_lfs_url_base=lambda: "https://cdn-lfs.huggingface.co",
    )
    return SimpleNamespace(state=SimpleNamespace(app_settings=SimpleNamespace(config=config)))


def test_get_request_ranges_defaults_to_full_file_and_zero_length():
    assert proxy_files.get_request_ranges(5, None) == ("bytes", [(0, 5)], None)
    assert proxy_files.get_request_ranges(0, None) == ("bytes", [], None)


def test_get_contiguous_ranges_splits_cached_and_remote_segments():
    class FakeCache:
        def _get_block_size(self):
            return 4

        def _get_file_size(self):
            return 12

        def has_block(self, idx):
            return idx == 1

    assert proxy_files.get_contiguous_ranges(FakeCache(), 0, 12) == [
        ((0, 4), True),
        ((4, 8), False),
        ((8, 12), True),
    ]


@pytest.mark.asyncio
async def test_file_realtime_stream_returns_invalid_data_error_on_bad_pathsinfo(monkeypatch, tmp_path):
    async def fake_pathsinfo_generator(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body("not-json"),
        )

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)

    result = await proxy_files._file_realtime_stream(
        app=_make_app(tmp_path),
        repo_type="models",
        org="team",
        repo="demo",
        file_path="file.bin",
        save_path=str(tmp_path / "save"),
        head_path=str(tmp_path / "head"),
        url="http://localhost/file.bin",
        request=_make_request(),
        method="GET",
        allow_cache=True,
        commit="main",
    )
    body = [chunk async for chunk in result.body]

    assert result.status_code == 504
    assert result.headers["x-error-code"] == "ProxyInvalidData"
    assert body == [b""]


@pytest.mark.asyncio
async def test_file_realtime_stream_handles_empty_and_ambiguous_pathsinfo(monkeypatch, tmp_path):
    async def empty_pathsinfo(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body("[]"),
        )

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", empty_pathsinfo)

    async def collect_response():
        result = await proxy_files._file_realtime_stream(
            app=_make_app(tmp_path),
            repo_type="models",
            org="team",
            repo="demo",
            file_path="file.bin",
            save_path=str(tmp_path / "save"),
            head_path=str(tmp_path / "head"),
            url="http://localhost/file.bin",
            request=_make_request(),
            method="GET",
            allow_cache=True,
            commit="main",
        )
        body = [chunk async for chunk in result.body]
        return result.status_code, result.headers, body

    status, headers, _ = await collect_response()
    assert status == 404
    assert headers["x-error-code"] == "EntryNotFound"

    async def multiple_pathsinfo(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(json.dumps([{"size": 1}, {"size": 2}])),
        )

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", multiple_pathsinfo)

    status, headers, _ = await collect_response()
    assert status == 504
    assert headers["x-error-code"] == "ProxyTimeout"


@pytest.mark.asyncio
async def test_file_realtime_stream_builds_headers_and_streams_get_chunks(monkeypatch, tmp_path):
    captured = {}

    async def fake_pathsinfo_generator(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(json.dumps([{"size": 10}])),
        )

    async def fake_resource_etag(hf_url, authorization=None, offline=False):
        captured["hf_url"] = hf_url
        captured["authorization"] = authorization
        captured["offline"] = offline
        return '"etag-123"'

    async def fake_file_chunk_get(**kwargs):
        captured["chunk_get"] = kwargs
        yield b"abc"
        yield b"def"

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_resource_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_get", fake_file_chunk_get)

    result = await proxy_files._file_realtime_stream(
        app=_make_app(tmp_path),
        repo_type="models",
        org="team",
        repo="demo",
        file_path="file.bin",
        save_path=str(tmp_path / "save"),
        head_path=str(tmp_path / "head"),
        url="https://mirror.example/file.bin?download=1",
        request=_make_request("GET", headers={"range": "bytes=2-5", "authorization": "Bearer t", "host": "mirror.example"}),
        method="GET",
        allow_cache=False,
        commit="abc123",
    )
    body = [chunk async for chunk in result.body]

    assert result.status_code == 206
    assert result.headers["accept-ranges"] == "bytes"
    assert result.headers["content-length"] == "4"
    assert result.headers["content-range"] == "bytes 2-5/10"
    assert result.headers["etag"] == '"etag-123"'
    assert result.headers["x-repo-commit"] == "abc123"
    assert body == [b"abc", b"def"]
    assert captured["hf_url"] == "https://cdn-lfs.huggingface.co/file.bin?download=1"
    assert captured["authorization"] == "Bearer t"
    assert captured["chunk_get"]["headers"]["host"] == "cdn-lfs.huggingface.co"
    assert captured["chunk_get"]["allow_cache"] is False


@pytest.mark.asyncio
async def test_file_realtime_stream_uses_head_stream_for_head_requests(monkeypatch, tmp_path):
    async def fake_pathsinfo_generator(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(json.dumps([{"size": 3}])),
        )

    async def fake_resource_etag(*args, **kwargs):
        return '"etag-head"'

    async def fake_file_chunk_head(**kwargs):
        yield b"ignored-head-body"

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_resource_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_head", fake_file_chunk_head)

    result = await proxy_files._file_realtime_stream(
        app=_make_app(tmp_path),
        repo_type="models",
        org="team",
        repo="demo",
        file_path="file.bin",
        save_path=str(tmp_path / "save"),
        head_path=str(tmp_path / "head"),
        url="https://huggingface.co/team/demo/resolve/main/file.bin",
        request=_make_request("HEAD"),
        method="HEAD",
        allow_cache=True,
        commit="main",
    )
    body = [chunk async for chunk in result.body]

    assert result.status_code == 200
    assert result.headers["content-length"] == "3"
    assert result.headers["accept-ranges"] == "bytes"
    assert "content-range" not in result.headers
    assert result.headers["etag"] == '"etag-head"'
    assert body == [b"ignored-head-body"]


@pytest.mark.asyncio
async def test_file_realtime_stream_without_repo_context_uses_remote_metadata(monkeypatch, tmp_path):
    captured = {}

    async def fail_pathsinfo(*args, **kwargs):
        raise AssertionError("pathsinfo should not be used without repo context")

    async def fake_remote_file_metadata(app, hf_url, authorization, offline):
        captured["metadata"] = {
            "hf_url": hf_url,
            "authorization": authorization,
            "offline": offline,
        }
        return proxy_files.RemoteFileMetadata(file_size=6, etag='"cdn-etag"')

    async def fake_file_chunk_get(**kwargs):
        captured["chunk_get"] = kwargs
        yield b"hello!"

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fail_pathsinfo)
    monkeypatch.setattr(proxy_files, "_remote_file_metadata", fake_remote_file_metadata)
    monkeypatch.setattr(proxy_files, "_file_chunk_get", fake_file_chunk_get)

    result = await proxy_files._file_realtime_stream(
        app=_make_app(tmp_path),
        save_path=str(tmp_path / "save"),
        head_path=str(tmp_path / "head"),
        url="https://mirror.example/team/demo/hash.bin",
        request=_make_request("GET", headers={"authorization": "Bearer t", "host": "mirror.example"}),
        method="GET",
        allow_cache=False,
    )
    body = [chunk async for chunk in result.body]

    assert result.status_code == 200
    assert result.headers["accept-ranges"] == "bytes"
    assert result.headers["content-length"] == "6"
    assert result.headers["etag"] == '"cdn-etag"'
    assert body == [b"hello!"]
    assert captured["metadata"]["hf_url"] == "https://cdn-lfs.huggingface.co/team/demo/hash.bin"
    assert captured["chunk_get"]["headers"]["host"] == "cdn-lfs.huggingface.co"


@pytest.mark.asyncio
async def test_cdn_and_lfs_generators_use_shared_stream_builder(monkeypatch, tmp_path):
    captured = []

    async def fake_file_realtime_stream(**kwargs):
        captured.append(kwargs)
        return ProxyResult(
            status_code=200,
            headers={"etag": '"ok"'},
            body=single_chunk_body(b""),
        )

    monkeypatch.setattr(proxy_files, "_file_realtime_stream", fake_file_realtime_stream)

    async def fake_check_cache_rules_hf(*args, **kwargs):
        return True

    monkeypatch.setattr(proxy_files, "check_cache_rules_hf", fake_check_cache_rules_hf)

    request = _make_request("GET", headers={"host": "mirror.example"}, url_path="/team/demo/hash.bin")
    app = _make_app(tmp_path)

    cdn_result = await proxy_files.cdn_file_get_generator(
        app=app,
        repo_type="models",
        org="team",
        repo="demo",
        file_hash="hash.bin",
        method="GET",
        request=request,
    )
    assert cdn_result.status_code == 200

    from olah.proxy import lfs as proxy_lfs

    monkeypatch.setattr(proxy_lfs, "_file_realtime_stream", fake_file_realtime_stream)
    lfs_result = await proxy_lfs.lfs_get_generator(
        app=app,
        dir1="aa",
        dir2="bb",
        hash_repo="repohash",
        hash_file="filehash",
        request=request,
    )
    assert lfs_result.status_code == 200

    assert captured[0]["url"] == "http://mirror.example/team/demo/hash.bin"
    assert captured[0].get("repo_type") is None
    assert captured[0]["allow_cache"] is True
    assert captured[1]["url"] == "http://mirror.example/team/demo/hash.bin"
    assert captured[1].get("repo_type") is None
    assert captured[1]["allow_cache"] is False


@pytest.mark.asyncio
async def test_file_realtime_stream_builds_multipart_response_for_multiple_ranges(monkeypatch, tmp_path):
    captured = []

    async def fake_pathsinfo_generator(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(json.dumps([{"size": 8}])),
        )

    async def fake_resource_etag(*args, **kwargs):
        return '"etag-multi"'

    async def fake_file_chunk_get(**kwargs):
        captured.append(kwargs["headers"]["range"])
        if kwargs["headers"]["range"] == "bytes=0-1":
            yield b"AB"
        elif kwargs["headers"]["range"] == "bytes=4-5":
            yield b"EF"
        else:
            raise AssertionError(f"unexpected range {kwargs['headers']['range']}")

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_resource_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_get", fake_file_chunk_get)

    result = await proxy_files._file_realtime_stream(
        app=_make_app(tmp_path),
        repo_type="models",
        org="team",
        repo="demo",
        file_path="file.bin",
        save_path=str(tmp_path / "save"),
        head_path=str(tmp_path / "head"),
        url="https://mirror.example/file.bin",
        request=_make_request("GET", headers={"range": "bytes=0-1,4-5", "host": "mirror.example"}),
        method="GET",
        allow_cache=False,
        commit="abc123",
    )
    body_chunks = [
        chunk.encode("utf-8") if isinstance(chunk, str) else chunk
        async for chunk in result.body
    ]
    body = b"".join(body_chunks)

    assert result.status_code == 206
    assert result.headers["accept-ranges"] == "bytes"
    assert result.headers["content-type"].startswith('multipart/byteranges; boundary="')
    assert "content-range" not in result.headers
    boundary = result.headers["content-type"].split('boundary="', 1)[1][:-1]
    assert body.startswith(f"--{boundary}\r\n".encode("ascii"))
    assert b"Content-Range: bytes 0-1/8" in body
    assert b"Content-Range: bytes 4-5/8" in body
    assert b"AB" in body
    assert b"EF" in body
    assert body.endswith(f"--{boundary}--\r\n".encode("ascii"))
    assert int(result.headers["content-length"]) == len(body)
    assert captured == ["bytes=0-1", "bytes=4-5"]


@pytest.mark.asyncio
async def test_file_realtime_stream_rejects_unsatisfiable_ranges(monkeypatch, tmp_path):
    async def fake_pathsinfo_generator(*args, **kwargs):
        return ProxyResult(
            status_code=200,
            headers={"content-type": "application/json"},
            body=single_chunk_body(json.dumps([{"size": 3}])),
        )

    async def fake_resource_etag(*args, **kwargs):
        return '"etag-416"'

    monkeypatch.setattr(proxy_files, "pathsinfo_generator", fake_pathsinfo_generator)
    monkeypatch.setattr(proxy_files, "_resource_etag", fake_resource_etag)
    monkeypatch.setattr(proxy_files, "_file_chunk_get", pytest.fail)

    result = await proxy_files._file_realtime_stream(
        app=_make_app(tmp_path),
        repo_type="models",
        org="team",
        repo="demo",
        file_path="file.bin",
        save_path=str(tmp_path / "save"),
        head_path=str(tmp_path / "head"),
        url="https://mirror.example/file.bin",
        request=_make_request("GET", headers={"range": "bytes=5-9", "host": "mirror.example"}),
        method="GET",
        allow_cache=False,
        commit="abc123",
    )
    body = [chunk async for chunk in result.body]

    assert result.status_code == 416
    assert result.headers["accept-ranges"] == "bytes"
    assert result.headers["content-range"] == "bytes */3"
    assert body == [b""]


@pytest.mark.asyncio
async def test_get_file_range_from_cache_reads_sliced_bytes_across_blocks():
    class FakeCache:
        def _get_block_size(self):
            return 4

        def _get_file_size(self):
            return 8

        def has_block(self, idx):
            return idx in {0, 1}

        async def read_block(self, idx):
            return [b"ABCD", b"EFGH"][idx]

    chunks = [
        chunk
        async for chunk in proxy_files._get_file_range_from_cache(
            FakeCache(),
            start_pos=1,
            end_pos=7,
        )
    ]

    assert chunks == [b"BCD", b"EFG"]


@pytest.mark.asyncio
async def test_get_file_range_from_remote_streams_decompressed_content_and_sets_headers():
    captured = {}
    payload = b"remote-data"
    encoded = brotli.compress(payload)

    class FakeResponse:
        status_code = 206
        headers = {"content-encoding": "br"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def aiter_raw(self):
            yield encoded[:4]
            yield encoded[4:]

    class FakeClient:
        def stream(self, **kwargs):
            captured["stream"] = kwargs
            return FakeResponse()

    class FakeCache:
        def _get_file_size(self):
            return len(payload)

    remote_info = proxy_files.RemoteInfo(
        method="GET",
        url="https://huggingface.co/file.bin",
        headers={"authorization": "Bearer t"},
    )

    chunks = [
        chunk
        async for chunk in proxy_files._get_file_range_from_remote(
            client=FakeClient(),
            remote_info=remote_info,
            cache_file=FakeCache(),
            start_pos=0,
            end_pos=len(payload),
        )
    ]

    assert b"".join(chunks) == payload
    assert captured["stream"]["headers"]["authorization"] == "Bearer t"
    assert captured["stream"]["headers"]["range"] == f"bytes=0-{len(payload) - 1}"


@pytest.mark.asyncio
async def test_get_file_range_from_remote_raises_on_incomplete_content_length():
    class FakeResponse:
        status_code = 206
        headers = {"content-length": "3"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def aiter_raw(self):
            yield b"abc"

    class FakeClient:
        def stream(self, **kwargs):
            return FakeResponse()

    class FakeCache:
        def _get_file_size(self):
            return 10

    remote_info = proxy_files.RemoteInfo("GET", "https://huggingface.co/file.bin", {})

    with pytest.raises(Exception, match="incomplete"):
        chunks = [
            chunk
            async for chunk in proxy_files._get_file_range_from_remote(
                client=FakeClient(),
                remote_info=remote_info,
                cache_file=FakeCache(),
                start_pos=0,
                end_pos=4,
            )
        ]
        assert chunks == [b"abc"]
