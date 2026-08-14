# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Literal, Optional, Tuple
from fastapi import Request, Response
import fastapi.concurrency
import httpx
import portalocker
from urllib.parse import urlparse, urljoin

from olah.constants import (
    CHUNK_SIZE,
    WORKER_API_TIMEOUT,
    HUGGINGFACE_HEADER_X_REPO_COMMIT,
    HUGGINGFACE_HEADER_X_LINKED_ETAG,
    HUGGINGFACE_HEADER_X_LINKED_SIZE,
    ORIGINAL_LOC,
)
from olah.cache.olah_cache import (
    DEFAULT_BLOCK_SIZE,
    DEFAULT_CHUNK_SIZE,
    CacheIntegrityError,
    OlahCache,
    compression_algo_from_name,
)
from olah.errors import error_entry_not_found, error_proxy_invalid_data, error_proxy_timeout
from olah.proxy.pathsinfo import pathsinfo_generator
from olah.utils.cache_utils import read_cache_request, write_cache_request
from olah.utils.url_utils import (
    RemoteInfo,
    add_query_param,
    check_url_has_param_name,
    get_all_ranges,
    get_url_param_name,
    get_url_tail,
    parse_range_params,
    remove_query_param,
)
from olah.utils.repo_utils import get_org_repo
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.lfs_object_index import register_lfs_object, register_xet_object
from olah.utils.file_utils import make_dirs
from olah.constants import CHUNK_SIZE, LFS_FILE_BLOCK, WORKER_API_TIMEOUT
from olah.utils.zip_utils import Decompressor, decompress_data
from olah.proxy.result import ProxyResult, single_chunk_body


XET_RESPONSE_HEADERS = (
    "x-xet-hash",
    "x-xet-refresh-route",
    "x-linked-size",
    "x-linked-etag",
    "link",
)


@dataclass(frozen=True)
class RemoteFileMetadata:
    file_size: int
    etag: Optional[str]
    xet_headers: Optional[Dict[str, str]] = None


def get_block_info(pos: int, block_size: int, file_size: int) -> Tuple[int, int, int]:
    cur_block = pos // block_size
    block_start_pos = cur_block * block_size
    block_end_pos = min((cur_block + 1) * block_size, file_size)
    return cur_block, block_start_pos, block_end_pos


def get_contiguous_ranges(
    cache_file: OlahCache, start_pos: int, end_pos: int
) -> List[Tuple[Tuple[int, int], bool]]:
    start_block = start_pos // cache_file._get_block_size()
    end_block = (end_pos - 1) // cache_file._get_block_size()

    range_start_pos = start_pos
    range_is_remote = not cache_file.is_block_cached(start_block)
    cur_pos = start_pos
    # Get contiguous ranges: (range_start_pos, range_end_pos), is_remote
    ranges_and_cache_list: List[Tuple[Tuple[int, int], bool]] = []
    for cur_block in range(start_block, end_block + 1):
        cur_block, block_start_pos, block_end_pos = get_block_info(
            cur_pos, cache_file._get_block_size(), cache_file._get_file_size()
        )

        if cache_file.is_block_cached(cur_block):
            cur_is_remote = False
        else:
            cur_is_remote = True
        if range_is_remote != cur_is_remote:
            if range_start_pos < cur_pos:
                ranges_and_cache_list.append(
                    ((range_start_pos, cur_pos), range_is_remote)
                )
            range_start_pos = cur_pos
            range_is_remote = cur_is_remote
        cur_pos = block_end_pos

    ranges_and_cache_list.append(((range_start_pos, end_pos), range_is_remote))
    range_start_pos = end_pos
    return ranges_and_cache_list


def get_request_ranges(
    file_size: int, range_header: Optional[str]
) -> Tuple[str, List[Tuple[int, int]], Optional[int]]:
    if range_header is None:
        if file_size == 0:
            return "bytes", [], None
        range_header = f"bytes={0}-{file_size-1}"

    unit, ranges, suffix = parse_range_params(range_header)
    all_ranges = get_all_ranges(file_size, unit, ranges, suffix)
    return unit, all_ranges, suffix


def _single_range_header(start_pos: int, end_pos: int, file_size: int) -> str:
    return f"bytes {start_pos}-{end_pos - 1}/{file_size}"


def _multipart_boundary(etag: Optional[str], all_ranges: List[Tuple[int, int]], file_size: int) -> str:
    boundary_seed = f"{etag or ''}:{file_size}:{all_ranges}".encode("utf-8")
    return hashlib.sha256(boundary_seed).hexdigest()[:32]


def _multipart_part_header(boundary: str, start_pos: int, end_pos: int, file_size: int) -> bytes:
    return (
        f"--{boundary}\r\n"
        f"Content-Type: application/octet-stream\r\n"
        f"Content-Range: {_single_range_header(start_pos, end_pos, file_size)}\r\n"
        "\r\n"
    ).encode("ascii")


def _multipart_content_length(boundary: str, all_ranges: List[Tuple[int, int]], file_size: int) -> int:
    total = 0
    for start_pos, end_pos in all_ranges:
        total += len(_multipart_part_header(boundary, start_pos, end_pos, file_size))
        total += end_pos - start_pos
        total += len(b"\r\n")
    total += len(f"--{boundary}--\r\n".encode("ascii"))
    return total


async def _write_block_safely(
    cache_file: OlahCache,
    block_index: int,
    raw_block: bytes,
    allow_cache: bool,
) -> None:
    if not allow_cache:
        return
    if cache_file.has_block(block_index):
        return
    write_task = asyncio.create_task(cache_file.write_block(block_index, raw_block))
    try:
        await asyncio.shield(write_task)
    except asyncio.CancelledError:
        await write_task
        raise


async def _get_file_range_from_remote(
    client: httpx.AsyncClient,
    remote_info: RemoteInfo,
    cache_file: OlahCache,
    start_pos: int,
    end_pos: int,
):
    headers = {}
    if remote_info.headers.get("authorization", None) is not None:
        headers["authorization"] = remote_info.headers.get("authorization", None)
    headers["range"] = f"bytes={start_pos}-{end_pos - 1}"

    chunk_bytes = 0
    decompressor: Optional[Decompressor] = None
    async with client.stream(
        method=remote_info.method,
        url=remote_info.url,
        headers=headers,
        timeout=WORKER_API_TIMEOUT,
        follow_redirects=True,
    ) as response:
        status_code = response.status_code
    
        if status_code == 429:
            raise Exception("Too many requests in a given amount of time.")
        
        is_compressed = "content-encoding" in response.headers
        if is_compressed:
            decompressor = Decompressor(response.headers["content-encoding"].split(","))
        
        async for raw_chunk in response.aiter_raw():
            if not raw_chunk:
                continue
            if is_compressed and decompressor is not None:
                real_chunk = decompressor.decompress(raw_chunk)
                yield real_chunk
                chunk_bytes += len(real_chunk)
            else:
                yield raw_chunk
                chunk_bytes += len(raw_chunk)

        if is_compressed or "content-length" not in response.headers:
            response_content_length = chunk_bytes
        else:
            response_content_length = int(response.headers["content-length"])

    # Post check
    if end_pos - start_pos != response_content_length:
        raise Exception(
            f"The content of the response is incomplete. File size: {cache_file._get_file_size()}. Start-end: {start_pos}-{end_pos}. Expected-{end_pos - start_pos}. Accepted-{response_content_length}"
        )


# ---------------------------------------------------------------------------
# Per-block single-flight download coordination
# ---------------------------------------------------------------------------
# Concurrent requests (across coroutines AND across uvicorn worker processes)
# for the same uncached block share ONE upstream download. The leader wins a
# non-blocking exclusive advisory lock on a per-block sidecar (``<block>.dl.lock``)
# and publishes the block; followers wait for that lock, then serve from cache.
# If the leader dies before publishing, the OS releases its lock and a follower
# becomes the new leader (bounded retry).

def _try_lock_ex(path: str):
    """Acquire a non-blocking exclusive advisory lock on ``path``.

    Returns the open file handle (caller passes it to ``_release_lock``), or
    ``None`` if another process currently holds the lock. The lock file is
    created on demand and used ONLY as a lock target.
    """
    fh = open(path, "a+")
    try:
        portalocker.lock(fh, portalocker.LOCK_EX | portalocker.LOCK_NB)
        return fh
    except portalocker.LockException:
        fh.close()
        return None


def _release_lock(fh) -> None:
    """Release an advisory lock acquired via ``_try_lock_ex``."""
    try:
        portalocker.unlock(fh)
    except Exception:
        pass
    finally:
        try:
            fh.close()
        except Exception:
            pass


def _wait_lock_sh(path: str, timeout: float) -> bool:
    """Poll for a shared advisory lock (up to ``timeout`` seconds).

    Returns ``True`` once the leader's exclusive lock is released (acquire +
    immediately release), or ``False`` on timeout. Uses non-blocking probes so it
    works uniformly across portalocker versions; the sleeps run inside a
    threadpool so the event loop is never blocked.
    """
    deadline = time.time() + timeout
    while True:
        fh = open(path, "a+")
        try:
            portalocker.lock(fh, portalocker.LOCK_SH | portalocker.LOCK_NB)
            _release_lock(fh)
            return True
        except portalocker.LockException:
            fh.close()
            if time.time() >= deadline:
                return False
            time.sleep(0.05)


async def _download_full_block(
    client: httpx.AsyncClient,
    remote_info: RemoteInfo,
    cache_file: OlahCache,
    block_start: int,
    block_end: int,
) -> bytes:
    """Download exactly ``[block_start, block_end)`` from upstream.

    Thin collector over ``_get_file_range_from_remote`` (which already verifies
    the received byte count). Returns the real (unpadded) block bytes.
    """
    out = bytearray()
    async for chunk in _get_file_range_from_remote(
        client, remote_info, cache_file, block_start, block_end
    ):
        if chunk:
            out += chunk
    return bytes(out)


async def _read_block_real_payload(
    cache_file: OlahCache, block_index: int, real_len: int
) -> bytes:
    """Read a cached block and return its first ``real_len`` (unpadded) bytes.

    ``read_block`` verifies chunk CRCs but does NOT auto-invalidate on failure
    (unlike ``stream_range``), so on ``CacheIntegrityError`` the block is dropped
    here and the caller treats it as a miss.
    """
    try:
        padded = await cache_file.read_block(block_index)
    except CacheIntegrityError:
        await cache_file._invalidate_block(block_index)
        raise
    if padded is None:
        # Block was evicted between has_block() and the read. Treat it like a
        # corrupt block so the caller's single-flight path re-downloads it.
        raise CacheIntegrityError(f"Block {block_index} vanished before read.")
    return padded[:real_len]


async def _fetch_block_single_flight(
    *,
    client: httpx.AsyncClient,
    remote_info: RemoteInfo,
    cache_file: OlahCache,
    block_index: int,
    block_start: int,
    block_end: int,
    allow_cache: bool,
) -> bytes:
    """Return the real (unpadded) payload of ``block_index``, single-flighted.

    * Cached + CRC-valid -> serve from cache.
    * Caching disabled -> download the range with no coordination.
    * Otherwise coordinate via a per-block cross-process advisory lock: the
      leader downloads + publishes (under ``asyncio.shield`` so a client
      disconnect still lands the block); followers wait, then serve from cache.
      A leader that dies before publishing is retried by a follower (bounded).
    """
    bs = cache_file._get_block_size()
    real_len = block_end - block_start

    # Fast path: serve from cache. A corrupt block is invalidated and re-fetched.
    if cache_file.has_block(block_index):
        try:
            return await _read_block_real_payload(cache_file, block_index, real_len)
        except CacheIntegrityError:
            pass

    if not allow_cache:
        return await _download_full_block(
            client, remote_info, cache_file, block_start, block_end
        )

    dl_lock_path = cache_file.get_block_path(block_index) + ".dl.lock"
    max_attempts = 3
    for _ in range(max_attempts):
        # Try to BECOME the leader (non-blocking exclusive lock), off the event loop.
        lock_fh = await fastapi.concurrency.run_in_threadpool(_try_lock_ex, dl_lock_path)
        if lock_fh is not None:
            try:
                # Double-check under EX: a prior leader may have just published.
                if cache_file.has_block(block_index):
                    try:
                        return await _read_block_real_payload(
                            cache_file, block_index, real_len
                        )
                    except CacheIntegrityError:
                        pass  # corrupt -> re-download below
                raw_real = await _download_full_block(
                    client, remote_info, cache_file, block_start, block_end
                )
                # write_block requires a full block_size buffer; pad the final block.
                raw_block = (
                    raw_real if real_len == bs else raw_real + b"\x00" * (bs - real_len)
                )
                # _write_block_safely shields the publish so a leader disconnect
                # still lands the block (followers depend on it). If the entry was
                # evicted mid-download (hourly cleanup removed blocks/), publishing
                # is impossible -- treat it as best-effort and still return the
                # fetched bytes so this client succeeds; a future request recreates
                # the entry.
                try:
                    await _write_block_safely(cache_file, block_index, raw_block, allow_cache=True)
                except FileNotFoundError:
                    pass
                return raw_real
            finally:
                _release_lock(lock_fh)
        # Someone else is the leader: wait for it to finish, then serve from cache.
        await fastapi.concurrency.run_in_threadpool(_wait_lock_sh, dl_lock_path, 120)
        if cache_file.has_block(block_index):
            try:
                return await _read_block_real_payload(cache_file, block_index, real_len)
            except CacheIntegrityError:
                pass  # corrupt or leader died mid-publish -> retry as leader
        # Block still absent -> leader died before publishing. Loop and try to
        # become the new leader.
    raise Exception(
        f"block {block_index} never became available after {max_attempts} single-flight attempts"
    )


async def _yield_range_blocks(
    *,
    client: httpx.AsyncClient,
    remote_info: RemoteInfo,
    cache_file: OlahCache,
    range_start_pos: int,
    range_end_pos: int,
    resume_pos: int,
    allow_cache: bool,
) -> AsyncIterator[bytes]:
    """Yield the client-requested slice of each block in ``[resume_pos, range_end_pos)``.

    Shared by the cache-MISS path (``resume_pos == range_start_pos``) and by the
    cache-HIT recovery path (``resume_pos`` == where ``stream_range`` failed). Each
    block goes through per-block single-flight, so a block evicted mid-response is
    transparently re-fetched and the client still gets a complete, correct response.
    """
    bs = cache_file._get_block_size()
    fs = cache_file._get_file_size()
    first_block = resume_pos // bs
    last_block = (range_end_pos - 1) // bs
    for blk in range(first_block, last_block + 1):
        block_start = blk * bs
        block_end = min((blk + 1) * bs, fs)
        block_bytes = await _fetch_block_single_flight(
            client=client,
            remote_info=remote_info,
            cache_file=cache_file,
            block_index=blk,
            block_start=block_start,
            block_end=block_end,
            allow_cache=allow_cache,
        )
        lo = max(resume_pos, block_start) - block_start
        hi = min(range_end_pos, block_end) - block_start
        piece = block_bytes[lo:hi]
        if piece:
            yield piece


async def _file_chunk_get(
    app,
    save_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: Optional[str],
    headers: Dict[str, str],
    allow_cache: bool,
    file_size: int,
    expected_etag: Optional[str] = None,
):
    # Redirect Chunks
    cfg = app.state.app_settings.config
    compression_algo = compression_algo_from_name(cfg.cache_compression)
    block_size = cfg.cache_block_size or DEFAULT_BLOCK_SIZE
    chunk_size = cfg.cache_chunk_size or DEFAULT_CHUNK_SIZE
    # Opening revalidates identity online: a non-None expected_etag that differs
    # from the stored one wipes & recreates the cache. Offline callers pass None
    # to trust the disk. The constructor handles create / reuse / wipe uniformly
    # and sizes chunks.crc for new caches.
    # Opening a cache does mkdirs + advisory locking + (on create) fsync, all of
    # which would block the event loop -- run it in the threadpool. Opening also
    # bumps the entry's meta.lock mtime, which is the LRU access signal eviction
    # sorts on (see olah.utils.disk_utils.collect_cache_units).
    cache_file = await fastapi.concurrency.run_in_threadpool(
        OlahCache,
        save_path,
        file_size=file_size,
        block_size=block_size,
        chunk_size=chunk_size,
        compression_algo=compression_algo,
        expected_etag=expected_etag,
    )

    try:
        _, all_ranges, _ = get_request_ranges(file_size, headers.get("range"))

        for start_pos, end_pos in all_ranges:
            ranges_and_cache_list = get_contiguous_ranges(cache_file, start_pos, end_pos)
            # Stream ranges
            for (range_start_pos, range_end_pos), is_remote in ranges_and_cache_list:
                # range_start_pos is zero-index and range_end_pos is exclusive
                if is_remote:
                    # Cache miss: fetch each missing block individually under a
                    # per-block single-flight lock so concurrent requests for the
                    # same uncached block share ONE upstream download (the leader
                    # downloads + publishes; followers wait then serve from cache).
                    if url is None:
                        # Cache-only mode (e.g. the Xet route serving a fully-cached
                        # object without re-resolving HF). A miss here means a block
                        # was evicted/corrupted between the pre-check and now; abort
                        # so the client retries and the route re-evaluates coverage.
                        raise Exception(
                            "cache miss in cache-only mode (no upstream URL available)"
                        )
                    remote_info = RemoteInfo(method, url, headers)
                    async for piece in _yield_range_blocks(
                        client=client,
                        remote_info=remote_info,
                        cache_file=cache_file,
                        range_start_pos=range_start_pos,
                        range_end_pos=range_end_pos,
                        resume_pos=range_start_pos,
                        allow_cache=allow_cache,
                    ):
                        yield piece
                else:
                    # Cache hit: blocks are already on disk -- no reassembly, no
                    # cache writes. OlahCache.stream_range serves both uncompressed
                    # (raw seek+read) and compressed (sub-chunk-indexed) blocks in
                    # ~1MB pieces, verifying each chunk's CRC. If a block vanishes
                    # mid-stream (eviction) or fails CRC, stream_range raises
                    # CacheIntegrityError; we fall back to per-block single-flight
                    # for the unsent remainder so the client still gets a complete,
                    # correct response instead of a truncated 500.
                    cur_pos = range_start_pos
                    try:
                        async for piece in cache_file.stream_range(
                            range_start_pos, range_end_pos
                        ):
                            if piece:
                                yield piece
                                cur_pos += len(piece)
                    except CacheIntegrityError:
                        if url is None:
                            # Cache-only mode cannot re-fetch; let it surface.
                            raise
                        remote_info = RemoteInfo(method, url, headers)
                        async for piece in _yield_range_blocks(
                            client=client,
                            remote_info=remote_info,
                            cache_file=cache_file,
                            range_start_pos=range_start_pos,
                            range_end_pos=range_end_pos,
                            resume_pos=cur_pos,
                            allow_cache=allow_cache,
                        ):
                            if piece:
                                yield piece
                                cur_pos += len(piece)
                    if cur_pos != range_end_pos:
                        raise Exception(
                            f"The size of cached range ({range_end_pos - range_start_pos}) is different from sent size ({cur_pos - range_start_pos})."
                        )
    finally:
        await fastapi.concurrency.run_in_threadpool(cache_file.close)


async def _stream_single_range(
    app,
    save_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: Optional[str],
    headers: Dict[str, str],
    allow_cache: bool,
    file_size: int,
    expected_etag: Optional[str] = None,
    requested_range: Optional[Tuple[int, int]] = None,
) -> AsyncIterator[bytes]:
    range_headers = dict(headers)
    if requested_range is None:
        range_headers.pop("range", None)
    else:
        start_pos, end_pos = requested_range
        range_headers["range"] = f"bytes={start_pos}-{end_pos - 1}"

    async for chunk in _file_chunk_get(
        app=app,
        save_path=save_path,
        client=client,
        method=method,
        url=url,
        headers=range_headers,
        allow_cache=allow_cache,
        file_size=file_size,
        expected_etag=expected_etag,
    ):
        yield chunk


async def _file_chunk_head(
    app,
    save_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    allow_cache: bool,
    file_size: int,
):
    if not app.state.app_settings.config.offline:
        async with client.stream(
            method=method,
            url=url,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        ) as response:
            async for raw_chunk in response.aiter_raw():
                if not raw_chunk:
                    continue
                yield raw_chunk
    else:
        yield b""


async def _resource_etag(hf_url: str, authorization: Optional[str]=None, offline: bool = False) -> Optional[str]:
    ret_etag = None
    sha256_hash = hashlib.sha256()
    sha256_hash.update(hf_url.encode("utf-8"))
    content_hash = sha256_hash.hexdigest()
    if offline:
        ret_etag = f'"{content_hash[:32]}-10"'
    else:
        etag_headers = {}
        if authorization is not None:
            etag_headers["authorization"] = authorization
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method="head",
                    url=hf_url,
                    headers=etag_headers,
                    timeout=WORKER_API_TIMEOUT,
                )
            if "etag" in response.headers:
                ret_etag = response.headers["etag"]
            else:
                ret_etag = f'"{content_hash[:32]}-10"'
        except httpx.HTTPError:
            ret_etag = None
    return ret_etag


async def _remote_file_metadata(
    app,
    hf_url: str,
    authorization: Optional[str],
    offline: bool,
) -> Tuple[Optional[RemoteFileMetadata], Optional[int]]:
    """Fetch file metadata from the remote. Returns (metadata, upstream status).

    The status code is ``None`` when no upstream response was received (network
    error). Callers pass 4xx statuses through so clients see the upstream error
    instead of an opaque 504.
    """
    if offline:
        etag = await _resource_etag(hf_url=hf_url, authorization=authorization, offline=True)
        return RemoteFileMetadata(file_size=0, etag=etag), None

    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method="HEAD",
                url=hf_url,
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
                follow_redirects=True,
            )
    except (httpx.HTTPError, ValueError):
        return None, None
    if response.status_code >= 400:
        return None, response.status_code

    content_length = response.headers.get("content-length")
    if content_length is None:
        # Xet-only files have no content-length in the resolve HEAD; fall back to x-linked-size.
        content_length = response.headers.get("x-linked-size")
    if content_length is None:
        return None, response.status_code
    try:
        file_size = int(content_length)
    except ValueError:
        return None, response.status_code
    xet_headers = {h: response.headers[h] for h in XET_RESPONSE_HEADERS if h in response.headers}
    return (
        RemoteFileMetadata(
            file_size=file_size,
            etag=response.headers.get("etag") or response.headers.get("x-linked-etag"),
            xet_headers=xet_headers or None,
        ),
        response.status_code,
    )


def _strip_quotes(value: Optional[str]) -> Optional[str]:
    if value and len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


async def _try_redirect_to_content_route(
    app,
    resp: Optional[httpx.Response],
    repo_type: Optional[str],
    org: Optional[str],
    repo: Optional[str],
    file_path: Optional[str],
    commit: Optional[str],
) -> Optional[ProxyResult]:
    """Redirect-model hook: for Xet files, return a 302 to olah's Xet content route.

    ``resp`` is the already-probed upstream response from
    ``_probe_xet_resolve`` (shared with the pass-through decision, so a
    request pays at most one upstream HEAD). Returns ``None`` to fall through
    to normal proxying (no repo context, not a redirect, classic/plain file,
    or upstream error). The 302 deliberately omits ``x-xet-hash`` so hf_hub
    does not engage native Xet and instead downloads over HTTP from olah's
    content route.
    """
    import re

    if not (repo_type and repo and file_path and commit):
        return None
    if resp is None:
        return None
    if resp.status_code not in (301, 302, 303, 307, 308):
        return None

    location = resp.headers.get("location", "")
    xet_hash = resp.headers.get("x-xet-hash")
    oid = _strip_quotes(resp.headers.get("x-linked-etag"))
    try:
        size = int(resp.headers.get("x-linked-size") or resp.headers.get("content-length"))
    except (TypeError, ValueError):
        size = None

    if xet_hash and oid:
        await register_xet_object(
            app, repo_type, org, repo, file_path, commit, oid, xet_hash, size or 0
        )
        await register_lfs_object(app, repo_type, org, repo, oid)
        m = re.search(r"/xet-bridge-[a-z]+/([^/]+)/([^/?]+)", location)
        repo_hash = m.group(1) if m else "_"
        response_headers = {
            "location": f"/xet-bridge-us/{repo_hash}/{xet_hash}",
            "etag": f'"{xet_hash}"',
            "accept-ranges": "bytes",
        }
        if size is not None:
            response_headers["content-length"] = str(size)
            response_headers["x-linked-size"] = str(size)
        if oid:
            response_headers["x-linked-etag"] = f'"{oid}"'
        if commit:
            response_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
        return ProxyResult(
            status_code=302, headers=response_headers, body=single_chunk_body(b"")
        )

    # Classic LFS / plain redirect: not routed here yet -> proxy.
    return None

async def _probe_xet_resolve(
    hf_url: str,
    authorization: Optional[str],
) -> Optional[httpx.Response]:
    """HEAD the resolve URL following *relative* redirects only.

    This mirrors what ``huggingface_hub`` does (its own follower only chases
    same-host redirects) and stops at the first absolute 3xx — that response
    carries the Xet metadata (``x-xet-hash``) for renamed/canonical-redirected
    repos. Returns ``None`` on transport error or a redirect loop. The result
    is shared by the pass-through and redirect-model decisions so a request
    pays at most one upstream HEAD.
    """
    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    response = None
    current_url = hf_url
    try:
        async with httpx.AsyncClient() as client:
            for _ in range(5):
                response = await client.request(
                    method="HEAD",
                    url=current_url,
                    headers=headers,
                    timeout=WORKER_API_TIMEOUT,
                    follow_redirects=False,
                )
                if not (300 <= response.status_code < 400):
                    break
                location = response.headers.get("location")
                if location is None:
                    break
                if urlparse(location).netloc:
                    # Absolute redirect — this is where Xet metadata lives.
                    break
                # Use RFC 3986 reference resolution so the redirect's query
                # string (or absence thereof) replaces the base's, instead of
                # the half-baked path-only swap urlparse._replace would do.
                current_url = urljoin(current_url, location)
            else:
                return None
    except (httpx.HTTPError, ValueError):
        return None
    return response


def _xet_passthrough_result(
    response: Optional[httpx.Response],
    commit: Optional[str],
    min_size: int,
) -> Optional[ProxyResult]:
    """Decide Xet pass-through from a probed upstream response (pure).

    Xet files above ``min_size`` cannot stream through olah: hf_hub refuses
    plain-HTTP downloads over its hardcoded size cap, so the client's hf_xet
    must speak the Xet chunked protocol against xethub.hf.co directly. When
    the response is Xet and big enough, mirror the upstream Xet headers so the
    client engages its native Xet path. Small Xet files return ``None`` and
    keep flowing through olah's cache.
    """
    if response is None or "x-xet-hash" not in response.headers:
        return None
    try:
        size = int(
            response.headers.get("x-linked-size")
            or response.headers.get("content-length")
        )
    except (TypeError, ValueError):
        # Size unknown: assume the client needs the native path rather than
        # risking hf_hub's "file too large" error later in the download.
        size = None
    if size is not None and size < min_size:
        return None
    response_headers: Dict[str, str] = {"accept-ranges": "bytes"}
    for h in XET_RESPONSE_HEADERS:
        if h in response.headers:
            response_headers[h] = response.headers[h]
    # huggingface_hub's _httpx_follow_relative_redirects_with_backoff raises
    # KeyError on a 3xx response without a Location header. Upstream's Location
    # is an absolute CAS bridge URL — the client won't follow it (only relative
    # redirects are followed) but the header must still be present.
    for h in ("etag", "content-length", "content-type", "location"):
        if h in response.headers:
            response_headers[h] = response.headers[h]
    if commit is not None:
        response_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
    elif "x-repo-commit" in response.headers:
        response_headers["x-repo-commit"] = response.headers["x-repo-commit"]
    return ProxyResult(
        status_code=response.status_code,
        headers=response_headers,
        body=single_chunk_body(b""),
    )


async def _file_realtime_stream(
    app,
    save_path: str,
    url: str,
    request: Request,
    repo_type: Optional[Literal["models", "datasets", "spaces"]] = None,
    org: Optional[str] = None,
    repo: Optional[str] = None,
    file_path: Optional[str] = None,
    method="GET",
    allow_cache=True,
    commit: Optional[str] = None,
    expected_etag: Optional[str] = None,
) -> ProxyResult:
    async def error_result(response) -> ProxyResult:
        return ProxyResult(
            status_code=response.status_code,
            headers=response.headers,
            body=single_chunk_body(response.body),
        )

    if check_url_has_param_name(url, ORIGINAL_LOC):
        clean_url = remove_query_param(url, ORIGINAL_LOC)
        original_loc = get_url_param_name(url, ORIGINAL_LOC)

        hf_loc = urlparse(original_loc)
        if len(hf_loc.netloc) != 0:
            hf_url = urljoin(
                f"{hf_loc.scheme}://{hf_loc.netloc}", get_url_tail(clean_url)
            )
        else:
            hf_url = urljoin(
                app.state.app_settings.config.hf_lfs_url_base(), get_url_tail(clean_url)
            )
    else:
        if urlparse(url).netloc in [
            app.state.app_settings.config.hf_netloc,
            app.state.app_settings.config.hf_lfs_netloc,
        ]:
            hf_url = url
        else:
            hf_url = urljoin(
                app.state.app_settings.config.hf_lfs_url_base(), get_url_tail(url)
            )

    request_headers = {k: v for k, v in request.headers.items()}
    if "host" in request_headers:
        request_headers["host"] = urlparse(hf_url).netloc

    authorization = request.headers.get("authorization", None)
    # Xet handling, both routes opt-in and sharing ONE upstream HEAD:
    #
    #   pass-through (xet-passthrough): mirror upstream Xet metadata for files
    #   >= xet-passthrough-min-size so the client's hf_xet downloads them
    #   directly from xethub.hf.co — hf_hub refuses plain-HTTP downloads over
    #   its hardcoded size cap, so those files cannot flow through the cache.
    #
    #   redirect model (cache-redirect-model): 302 Xet files to olah's own
    #   content route so range requests hit the cache directly.
    #
    # With both features off (the default) no probe is made at all.
    cfg = app.state.app_settings.config
    xet_passthrough_on = (
        not cfg.offline and getattr(cfg, "xet_passthrough", False)
    )
    redirect_model_on = getattr(cfg, "cache_redirect_model", False)
    if xet_passthrough_on or redirect_model_on:
        probe = await _probe_xet_resolve(hf_url=hf_url, authorization=authorization)
        if xet_passthrough_on:
            passthrough = _xet_passthrough_result(
                probe, commit, getattr(cfg, "xet_passthrough_min_size", 0)
            )
            if passthrough is not None:
                return passthrough
        if redirect_model_on:
            redirect = await _try_redirect_to_content_route(
                app, probe, repo_type, org, repo, file_path, commit
            )
            if redirect is not None:
                return redirect
    # Canonical repos (org=None, e.g. "bert-base-uncased") are ordinary HF
    # repos; paths-info handles them the same as org/<repo> names.
    if repo_type is not None and repo is not None and file_path is not None and commit is not None:
        generator = await pathsinfo_generator(
            app,
            repo_type,
            org,
            repo,
            commit,
            [file_path],
            override_cache=False,
            method="post",
            authorization=authorization,
        )
        if generator.status_code != 200:
            return generator
        content = ""
        async for chunk in generator.body:
            content = chunk
            break
        try:
            pathsinfo = json.loads(content)
        except json.JSONDecodeError:
            return await error_result(error_proxy_invalid_data())

        if len(pathsinfo) == 0:
            return await error_result(error_entry_not_found())

        if len(pathsinfo) != 1:
            return await error_result(error_proxy_timeout())

        pathinfo = pathsinfo[0]
        if "size" not in pathinfo:
            return await error_result(error_proxy_timeout())
        file_size = pathinfo["size"]
        # Register LFS content identity so later LFS downloads (which carry only
        # the content hash, not the repo) can be authorized against this repo.
        lfs_info = pathinfo.get("lfs") if isinstance(pathinfo, dict) else None
        lfs_oid = lfs_info.get("oid") if isinstance(lfs_info, dict) else None
        xet_hash = pathinfo.get("xetHash") if isinstance(pathinfo, dict) else None
        if lfs_oid:
            await register_lfs_object(app, repo_type, org, repo, lfs_oid)
            if xet_hash:
                await register_xet_object(
                    app, repo_type, org, repo, file_path, commit, lfs_oid, xet_hash, file_size
                )
        # Content-addressed cache identity: for LFS/Xet files use the content
        # SHA-256 (lfs oid) -- integrity-verifiable and stable across commits --
        # instead of a URL-derived pseudo-etag. Fall back to _resource_etag for
        # plain (non-LFS) files.
        if lfs_oid:
            etag = f'"{lfs_oid}"'
        else:
            etag = await _resource_etag(
                hf_url=hf_url,
                authorization=authorization,
                offline=app.state.app_settings.config.offline,
            )
    else:
        metadata, upstream_status = await _remote_file_metadata(
            app=app,
            hf_url=hf_url,
            authorization=authorization,
            offline=app.state.app_settings.config.offline,
        )
        if metadata is None:
            # Pass the upstream verdict through: a missing/unauthorized file is
            # the client's problem, not a proxy timeout.
            if upstream_status == 404:
                return await error_result(error_entry_not_found())
            if upstream_status in (401, 403):
                return await error_result(Response(status_code=upstream_status))
            return await error_result(error_proxy_timeout())
        file_size = metadata.file_size
        etag = metadata.etag

    # An explicit expected_etag (e.g. the LFS/Xet content hash) overrides the
    # upstream-derived etag for both the response and cache revalidation.
    if expected_etag is not None:
        etag = expected_etag
    return await _build_file_response(
        app, save_path, request_headers, method, hf_url, file_size, etag, allow_cache, commit
    )


async def _build_file_response(
    app,
    save_path: str,
    request_headers: Dict[str, str],
    method: str,
    upstream_url: Optional[str],
    file_size: int,
    etag: Optional[str],
    allow_cache: bool,
    commit: Optional[str],
) -> ProxyResult:
    """Build the ranged response (headers + streaming body) for a file served
    from ``upstream_url`` and cached at ``save_path``.

    Shared by the resolve proxy (``upstream_url`` = HF resolve URL) and the Xet
    content route (``upstream_url`` = a freshly re-resolved signed xet URL). The
    cache identity is ``etag`` (content-addressed for LFS/Xet files).
    """
    response_headers: Dict[str, str] = {}
    range_header = request_headers.get("range")
    _, all_ranges, _ = get_request_ranges(file_size, range_header)
    response_headers["accept-ranges"] = "bytes"
    if commit is not None:
        response_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
    response_headers["etag"] = etag

    if etag is None:
        return ProxyResult(
            status_code=504,
            headers={"x-error-message": "Proxy Timeout"},
            body=single_chunk_body(b""),
        )

    if range_header is None:
        status_code = 200
        response_headers["content-length"] = str(file_size)
    elif len(all_ranges) == 0:
        response_headers["content-range"] = f"bytes */{file_size}"
        return ProxyResult(
            status_code=416,
            headers=response_headers,
            body=single_chunk_body(b""),
        )
    elif len(all_ranges) == 1:
        start_pos, end_pos = all_ranges[0]
        status_code = 206
        response_headers["content-length"] = str(end_pos - start_pos)
        response_headers["content-range"] = _single_range_header(start_pos, end_pos, file_size)
    else:
        boundary = _multipart_boundary(etag, all_ranges, file_size)
        status_code = 206
        response_headers["content-type"] = f'multipart/byteranges; boundary="{boundary}"'
        response_headers["content-length"] = str(
            _multipart_content_length(boundary, all_ranges, file_size)
        )

    # Identity passed to the cache for online revalidation. Offline trusts the
    # disk (None) so a transient upstream-derived pseudo-etag never destroys a
    # good cache while offline.
    cache_expected_etag = None if app.state.app_settings.config.offline else etag

    async def body_iter() -> AsyncIterator[bytes]:
        async with httpx.AsyncClient() as client:
            if method.lower() == "get":
                if range_header is None:
                    async for each_chunk in _stream_single_range(
                        app=app,
                        save_path=save_path,
                        client=client,
                        method=method,
                        url=upstream_url,
                        headers=request_headers,
                        allow_cache=allow_cache,
                        file_size=file_size,
                        expected_etag=cache_expected_etag,
                    ):
                        yield each_chunk
                elif len(all_ranges) == 1:
                    async for each_chunk in _stream_single_range(
                        app=app,
                        save_path=save_path,
                        client=client,
                        method=method,
                        url=upstream_url,
                        headers=request_headers,
                        allow_cache=allow_cache,
                        file_size=file_size,
                        expected_etag=cache_expected_etag,
                        requested_range=all_ranges[0],
                    ):
                        yield each_chunk
                else:
                    for start_pos, end_pos in all_ranges:
                        yield _multipart_part_header(boundary, start_pos, end_pos, file_size)
                        async for each_chunk in _stream_single_range(
                            app=app,
                            save_path=save_path,
                            client=client,
                            method=method,
                            url=upstream_url,
                            headers=request_headers,
                            allow_cache=allow_cache,
                            file_size=file_size,
                            expected_etag=cache_expected_etag,
                            requested_range=(start_pos, end_pos),
                        ):
                            yield each_chunk
                        yield b"\r\n"
                    yield f"--{boundary}--\r\n".encode("ascii")
            elif method.lower() == "head":
                async for each_chunk in _file_chunk_head(
                    app=app,
                    save_path=save_path,
                    client=client,
                    method=method,
                    url=upstream_url,
                    headers=request_headers,
                    allow_cache=allow_cache,
                    file_size=0,
                ):
                    yield each_chunk
            else:
                raise Exception(f"Unsupported method: {method}")

    return ProxyResult(status_code=status_code, headers=response_headers, body=body_iter())


async def file_get_generator(
    app,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    method: Literal["HEAD", "GET"],
    request: Request,
):
    org_repo = get_org_repo(org, repo)
    # save
    repos_path = app.state.app_settings.config.repos_path
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org_repo}/resolve/{commit}/{file_path}"
    )
    make_dirs(save_path)

    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    if repo_type == "models":
        url = urljoin(
            app.state.app_settings.config.hf_url_base(),
            f"/{org_repo}/resolve/{commit}/{file_path}",
        )
    else:
        url = urljoin(
            app.state.app_settings.config.hf_url_base(),
            f"/{repo_type}/{org_repo}/resolve/{commit}/{file_path}",
        )
    return await _file_realtime_stream(
        app=app,
        repo_type=repo_type,
        org=org,
        repo=repo,
        file_path=file_path,
        save_path=save_path,
        url=url,
        request=request,
        method=method,
        allow_cache=allow_cache,
        commit=commit,
    )


async def cdn_file_get_generator(
    app,
    repo_type: Literal["models", "datasets", "spaces"],
    org: str,
    repo: str,
    file_hash: str,
    method: Literal["HEAD", "GET"],
    request: Request,
):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    org_repo = get_org_repo(org, repo)
    # save
    repos_path = app.state.app_settings.config.repos_path
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org_repo}/cdn/{file_hash}"
    )
    make_dirs(save_path)

    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    return await _file_realtime_stream(
        app=app,
        save_path=save_path,
        url=str(request.url),
        request=request,
        method=method,
        allow_cache=allow_cache,
    )
