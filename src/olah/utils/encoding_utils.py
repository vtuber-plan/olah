# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Content-encoding helpers for API relay and cache replay.

Some API clients (notably llama.cpp's vendored cpp-httplib) are built without
any content decoders and treat a ``content-encoding`` header as a hard error.
Relayed API responses are therefore normalized to identity encoding: live
bodies are decoded while streaming, and cached copies written by older
versions -- stored still content-encoded -- are decoded when replayed.
"""

import logging
import zlib
from typing import Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

# Wire-level headers that stop describing the body once it is relayed
# identity-encoded (or a cookie header that must not leak through the proxy).
_HEADERS_NOT_RELAYED = ("content-encoding", "content-length", "transfer-encoding", "set-cookie")


class _GzipDecoder:
    # wbits 31 = 15 | 16: transparent gzip container handling (header and
    # multi-member streams) on top of the zlib core.
    def __init__(self):
        self._obj = zlib.decompressobj(zlib.MAX_WBITS | 16)

    def process(self, data: bytes) -> bytes:
        return self._obj.decompress(data)

    def finish(self) -> bytes:
        return self._obj.flush()


class _DeflateDecoder:
    """Deflate with a transparent zlib-header fallback on the first chunk."""

    def __init__(self):
        self._obj: Optional[zlib.decompressobj] = None
        self._pending = b""

    def process(self, data: bytes) -> bytes:
        if self._obj is None:
            self._pending += data
            if not self._pending:
                return b""
            try:
                self._obj = zlib.decompressobj()
                return self._obj.decompress(self._pending)
            except zlib.error:
                self._obj = zlib.decompressobj(-zlib.MAX_WBITS)
                return self._obj.decompress(self._pending)
        return self._obj.decompress(data)

    def finish(self) -> bytes:
        if self._obj is None:
            return b""
        return self._obj.flush()


class _BrotliDecoder:
    # brotli.Decompressor is one-shot, so the stream is buffered and decoded
    # in one pass on flush. Fine for API metadata payloads.
    def __init__(self):
        self._pending = b""

    def process(self, data: bytes) -> bytes:
        if data:
            self._pending += data
        return b""

    def finish(self) -> bytes:
        import brotli

        data, self._pending = self._pending, b""
        if not data:
            return b""
        return brotli.decompress(data)


class IncrementalDecoder:
    """Incrementally decodes one content-encoding of a byte stream."""

    def __init__(self, encoding: str):
        if encoding in ("gzip", "x-gzip"):
            self._impl = _GzipDecoder()
        elif encoding == "deflate":
            self._impl = _DeflateDecoder()
        elif encoding == "br":
            try:
                import brotli  # noqa: F401
            except ImportError:
                raise ValueError("brotli decoder is not installed")
            self._impl = _BrotliDecoder()
        elif encoding == "identity":
            self._impl = None
        else:
            raise ValueError(f"unsupported content-encoding: {encoding}")

    def push(self, data: bytes) -> bytes:
        if self._impl is None or not data:
            return bytes(data)
        return self._impl.process(data)

    def flush(self) -> bytes:
        if self._impl is None:
            return b""
        return self._impl.finish()


def make_decoder_chain(encoding: str) -> List[IncrementalDecoder]:
    """Incremental decoders for a ``content-encoding`` value, outermost first.

    An empty list means the body is relayed verbatim: either it is not
    content-encoded at all, or it uses an encoding without a decoder here
    (e.g. zstd), in which case the legacy raw-relay behavior is kept.
    """
    parts = [part.strip().lower() for part in (encoding or "").split(",") if part.strip()]
    parts = [part for part in parts if part != "identity"]
    chain: List[IncrementalDecoder] = []
    for part in reversed(parts):
        try:
            chain.append(IncrementalDecoder(part))
        except ValueError:
            logger.warning("No decoder for content-encoding %r; relaying the body verbatim", part)
            return []
    return chain


def push_decoder_chain(chain: List[IncrementalDecoder], data: bytes) -> bytes:
    out = bytes(data)
    for decoder in chain:
        out = decoder.push(out)
    return out


def flush_decoder_chain(chain: List[IncrementalDecoder]) -> bytes:
    tail = b""
    for decoder in reversed(chain):
        tail = decoder.push(tail) + decoder.flush()
    return tail


def normalize_relay_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    """Lowercased copy of ``headers`` with wire-level headers stripped.

    The result is safe to attach to a response whose body is
    identity-encoded: no ``content-encoding``/``content-length``/
    ``transfer-encoding`` claim that the actual bytes do not satisfy.
    """
    normalized = {key.lower(): value for key, value in headers.items()}
    for name in _HEADERS_NOT_RELAYED:
        normalized.pop(name, None)
    return normalized


def decode_cached_response(content: bytes, headers: Mapping[str, str]) -> Tuple[bytes, Dict[str, str]]:
    """Decode a cached body stored content-encoded and normalize its headers.

    Caches written before identity normalization hold the encoded bytes
    together with the upstream ``content-encoding`` header; replaying them
    verbatim breaks decoder-less clients. The body is decoded (encodings in
    ``content-encoding`` are peeled in reverse order) and the headers are
    normalized with the exact decoded ``content-length``. When the body
    cannot be decoded (unknown encoding or corrupt data) it is returned
    untouched with the original headers, preserving the legacy relay
    behavior rather than serving corrupted content.
    """
    normalized = {key.lower(): value for key, value in headers.items()}
    encoding = (normalized.get("content-encoding") or "").strip()
    chain = make_decoder_chain(encoding)
    if encoding and not chain:
        # Unsupported encoding (e.g. zstd): legacy verbatim relay.
        return bytes(content), {key: value for key, value in headers.items()}
    try:
        body = push_decoder_chain(chain, bytes(content)) + flush_decoder_chain(chain)
    except (ValueError, OSError, EOFError, zlib.error):
        logger.warning("Could not decode cached body (%s); relaying it verbatim", encoding)
        return bytes(content), {key: value for key, value in headers.items()}
    clean = normalize_relay_headers(headers)
    clean["content-length"] = str(len(body))
    return body, clean
