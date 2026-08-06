# coding=utf-8
import os
from typing import Union

import httpx

# Small JSON/HEAD/metadata calls (seconds).
DEFAULT_API_TIMEOUT = 15.0
# Idle read timeout between chunks while streaming large LFS files (seconds).
DEFAULT_STREAM_READ_TIMEOUT = 300.0
DEFAULT_STREAM_CONNECT_TIMEOUT = 30.0


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def worker_api_timeout() -> float:
    return _env_float("OLAH_API_TIMEOUT", DEFAULT_API_TIMEOUT)


def worker_stream_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=_env_float("OLAH_STREAM_CONNECT_TIMEOUT", DEFAULT_STREAM_CONNECT_TIMEOUT),
        read=_env_float("OLAH_STREAM_READ_TIMEOUT", DEFAULT_STREAM_READ_TIMEOUT),
        write=60.0,
        pool=30.0,
    )


def worker_http2_enabled() -> bool:
    # HTTP/1.1 is more stable for long Range downloads through reverse proxies.
    return _env_bool("OLAH_HTTP2", False)


def create_stream_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=worker_stream_timeout(),
        http2=worker_http2_enabled(),
        limits=httpx.Limits(
            max_keepalive_connections=_env_int("OLAH_HTTP_MAX_KEEPALIVE", 32),
            max_connections=_env_int("OLAH_HTTP_MAX_CONNECTIONS", 64),
        ),
    )


def create_api_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=worker_api_timeout(),
        http2=worker_http2_enabled(),
    )


def is_transient_upstream_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.TransportError):
        return True
    msg = str(exc).lower()
    for kw in (
        "429",
        "502",
        "503",
        "504",
        "connection reset",
        "connection refused",
        "broken pipe",
        "unexpected eof",
        "prematurely closed",
        "too many requests",
    ):
        if kw in msg:
            return True
    return False


def is_transient_upstream_status(status_code: int) -> bool:
    return status_code in (408, 429, 500, 502, 503, 504)


# Backward-compatible alias used across the codebase.
WORKER_API_TIMEOUT: Union[float, httpx.Timeout] = DEFAULT_API_TIMEOUT
