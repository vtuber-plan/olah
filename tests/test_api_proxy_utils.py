import gzip
import json

from olah.proxy.tree import build_hf_tree_url
from olah.utils.api_proxy_utils import build_upstream_api_headers, normalize_api_response


def test_build_upstream_api_headers_requests_identity():
    headers = build_upstream_api_headers("token-abc")
    assert headers["Accept-Encoding"] == "identity"
    assert headers["authorization"] == "token-abc"


def test_build_upstream_api_headers_without_token():
    headers = build_upstream_api_headers(None)
    assert headers == {"Accept-Encoding": "identity"}


def test_normalize_api_response_decompresses_gzip_header():
    payload = b'[{"path":"model.gguf","type":"file","size":123}]'
    encoded = gzip.compress(payload)
    content, headers = normalize_api_response(
        encoded,
        {"Content-Type": "application/json", "Content-Encoding": "gzip"},
    )
    assert content == payload
    assert "content-encoding" not in headers
    assert headers["content-length"] == str(len(payload))


def test_normalize_api_response_decompresses_gzip_magic_without_header():
    payload = b'{"ok": true}'
    encoded = gzip.compress(payload)
    content, headers = normalize_api_response(encoded, {"Content-Type": "application/json"})
    assert content == payload
    assert "content-encoding" not in headers


def test_normalize_api_response_passthrough_plain_json():
    payload = b'[{"path":"a.bin","type":"file"}]'
    content, headers = normalize_api_response(
        payload,
        {"Content-Type": "application/json", "Content-Length": str(len(payload))},
    )
    assert content == payload
    assert headers["content-type"] == "application/json"


def test_build_hf_tree_url_root_matches_hf_api():
    url = build_hf_tree_url(
        "https://huggingface.co",
        "models",
        "org/repo",
        "main",
        "",
    )
    assert url == "https://huggingface.co/api/models/org/repo/tree/main"


def test_build_hf_tree_url_subdirectory():
    url = build_hf_tree_url(
        "https://huggingface.co/",
        "models",
        "org/repo",
        "main",
        "weights/",
    )
    assert url == "https://huggingface.co/api/models/org/repo/tree/main/weights"


def test_normalize_api_response_roundtrip_json():
    entries = [{"path": "a.gguf", "type": "file", "size": 42}]
    raw = json.dumps(entries).encode("utf-8")
    content, _ = normalize_api_response(raw, {"Content-Type": "application/json"})
    assert json.loads(content.decode("utf-8")) == entries
