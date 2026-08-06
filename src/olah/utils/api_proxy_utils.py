# coding=utf-8
import gzip
from typing import Dict, Mapping, Optional, Tuple


def build_upstream_api_headers(authorization: Optional[str]) -> Dict[str, str]:
    """Headers for HuggingFace JSON API proxy calls."""
    headers: Dict[str, str] = {"Accept-Encoding": "identity"}
    if authorization is not None:
        headers["authorization"] = authorization
    return headers


def _normalize_header_keys(headers: Mapping[str, str]) -> Dict[str, str]:
    return {str(k).lower(): str(v) for k, v in headers.items()}


def normalize_api_response(
    content: bytes, headers: Mapping[str, str]
) -> Tuple[bytes, Dict[str, str]]:
    """
    Return plain JSON bytes and client-safe headers.

    Upstream may still respond with gzip even when identity is requested; cached
    entries can also retain Content-Encoding: gzip. Clients such as download-init
    expect uncompressed JSON when they send Accept-Encoding: identity.
    """
    out_headers = _normalize_header_keys(headers)
    encoding = out_headers.get("content-encoding", "").lower()
    if encoding == "gzip" or (
        len(content) >= 2 and content[0] == 0x1F and content[1] == 0x8B
    ):
        content = gzip.decompress(content)
        out_headers.pop("content-encoding", None)
        out_headers["content-length"] = str(len(content))
    return content, out_headers
