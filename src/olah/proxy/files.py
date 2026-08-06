# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Literal, Optional, Tuple
from fastapi import Request
import httpx
from urllib.parse import urlparse, urljoin

from olah.constants import (
    CHUNK_SIZE,
    LFS_FILE_BLOCK,
    OLAH_CACHE_SHA256_VERIFY,
    OLAH_REMOTE_FETCH_BLOCK_ALIGN,
    OLAH_REMOTE_RETRY_MAX,
)
from olah.cache.olah_cache import OlahCache
from olah.cache.integrity import lfs_sha256_from_pathsinfo, verify_cache_sha256
from olah.errors import error_entry_not_found, error_proxy_invalid_data, error_proxy_timeout
from olah.proxy.pathsinfo import pathsinfo_generator
from olah.utils.cache_utils import read_cache_request, write_cache_request
from olah.utils.disk_utils import touch_file_access_time
from olah.utils.http_utils import (
    create_stream_client,
    is_transient_upstream_error,
    is_transient_upstream_status,
    worker_api_timeout,
    worker_stream_timeout,
)
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
from olah.utils.file_utils import make_dirs
from olah.constants import (
    HUGGINGFACE_HEADER_X_REPO_COMMIT,
    HUGGINGFACE_HEADER_X_LINKED_ETAG,
    HUGGINGFACE_HEADER_X_LINKED_SIZE,
    ORIGINAL_LOC,
)
from olah.utils.zip_utils import Decompressor, decompress_data
from olah.proxy.result import ProxyResult, single_chunk_body

logger = logging.getLogger(__name__)

# One in-flight cache writer per file path — prevents concurrent block assembly races.
_file_cache_locks: Dict[str, asyncio.Lock] = {}
_file_cache_locks_guard = asyncio.Lock()


async def _acquire_file_cache_lock(save_path: str) -> asyncio.Lock:
    async with _file_cache_locks_guard:
        lock = _file_cache_locks.get(save_path)
        if lock is None:
            lock = asyncio.Lock()
            _file_cache_locks[save_path] = lock
    await lock.acquire()
    return lock


def _should_persist_block(
    cache_file: OlahCache,
    block_index: int,
    raw_block_len: int,
) -> bool:
    block_size = cache_file._get_block_size()
    if raw_block_len == block_size:
        return True
    if raw_block_len <= 0:
        return False
    if not cache_file.is_terminal_block(block_index):
        return False
    expected_len = cache_file._expected_decompressed_len(block_index)
    return raw_block_len == expected_len


def _pad_block_for_write(
    cache_file: OlahCache, block_index: int, raw_block: bytes
) -> bytes:
    block_size = cache_file._get_block_size()
    if len(raw_block) == block_size:
        return raw_block
    if cache_file.is_terminal_block(block_index):
        expected_len = cache_file._expected_decompressed_len(block_index)
        if len(raw_block) == expected_len:
            return raw_block + b"\x00" * (block_size - expected_len)
    raise ValueError(
        f"Refusing to persist block {block_index}: len {len(raw_block)} "
        f"is neither full block ({block_size}) nor a complete terminal block"
    )


@dataclass(frozen=True)
class RemoteFileMetadata:
    file_size: int
    etag: Optional[str]


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
    range_is_remote = not cache_file.has_block(start_block)
    cur_pos = start_pos
    # Get contiguous ranges: (range_start_pos, range_end_pos), is_remote
    ranges_and_cache_list: List[Tuple[Tuple[int, int], bool]] = []
    for cur_block in range(start_block, end_block + 1):
        cur_block, block_start_pos, block_end_pos = get_block_info(
            cur_pos, cache_file._get_block_size(), cache_file._get_file_size()
        )

        if cache_file.has_block(cur_block):
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


def iter_block_bounded_ranges(
    start_pos: int, end_pos: int, block_size: int
) -> List[Tuple[int, int]]:
    """Split [start_pos, end_pos) so each slice spans at most one cache block (<= block_size bytes)."""
    if start_pos >= end_pos:
        return []
    slices: List[Tuple[int, int]] = []
    pos = start_pos
    while pos < end_pos:
        block_end = min(((pos // block_size) + 1) * block_size, end_pos)
        slices.append((pos, block_end))
        pos = block_end
    return slices


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
    blocks_written: Optional[set] = None,
) -> None:
    if not allow_cache:
        return
    if not _should_persist_block(cache_file, block_index, len(raw_block)):
        return
    padded_block = _pad_block_for_write(cache_file, block_index, raw_block)
    if cache_file.has_block(block_index):
        return
    write_task = asyncio.create_task(
        cache_file.write_block(block_index, padded_block)
    )
    try:
        await asyncio.shield(write_task)
        if blocks_written is not None:
            blocks_written.add(block_index)
    except asyncio.CancelledError:
        await write_task
        raise


async def _get_file_range_from_cache(
    cache_file: OlahCache,
    start_pos: int,
    end_pos: int,
    client: Optional[httpx.AsyncClient] = None,
    remote_info: Optional[RemoteInfo] = None,
):
    start_block = start_pos // cache_file._get_block_size()
    end_block = (end_pos - 1) // cache_file._get_block_size()
    cur_pos = start_pos
    for cur_block in range(start_block, end_block + 1):
        _, block_start_pos, block_end_pos = get_block_info(
            cur_pos, cache_file._get_block_size(), cache_file._get_file_size()
        )
        if not cache_file.has_block(cur_block):
            raise Exception("Unknown exception: read block which has not been cached.")
        raw_block = await cache_file.read_block(cur_block)
        if raw_block is None:
            if client is None or remote_info is None:
                raise Exception(
                    f"Corrupt cache block {cur_block} invalidated and no remote fallback is configured."
                )
            refetch_start = max(start_pos, block_start_pos)
            refetch_end = min(end_pos, block_end_pos)
            async for chunk in _get_file_range_from_remote(
                client,
                remote_info,
                cache_file,
                refetch_start,
                refetch_end,
            ):
                yield chunk
            cur_pos = refetch_end
            continue
        chunk = raw_block[
            max(start_pos, block_start_pos)
            - block_start_pos : min(end_pos, block_end_pos)
            - block_start_pos
        ]
        yield chunk
        cur_pos += len(chunk)

    if cur_pos != end_pos:
        raise Exception("The cache range from {} to {} is incomplete.")


async def _get_file_range_from_remote(
    client: httpx.AsyncClient,
    remote_info: RemoteInfo,
    cache_file: OlahCache,
    start_pos: int,
    end_pos: int,
):
    expected_len = end_pos - start_pos
    last_err: Optional[BaseException] = None
    retry_max = int(os.getenv("OLAH_REMOTE_RETRY_MAX", str(OLAH_REMOTE_RETRY_MAX)))

    for attempt in range(1, retry_max + 1):
        if attempt > 1:
            cache_file.request_stream_reset(
                start_pos,
                end_pos,
                f"upstream range retry attempt {attempt}/{retry_max}",
            )
        collected = bytearray()
        try:
            async for chunk in _get_file_range_from_remote_once(
                client,
                remote_info,
                cache_file,
                start_pos,
                end_pos,
            ):
                collected.extend(chunk)
            if len(collected) != expected_len:
                raise Exception(
                    f"The content of the response is incomplete. "
                    f"File size: {cache_file._get_file_size()}. "
                    f"Start-end: {start_pos}-{end_pos}. "
                    f"Expected: {expected_len}. Accepted: {len(collected)}"
                )
            for offset in range(0, len(collected), CHUNK_SIZE):
                yield bytes(collected[offset : offset + CHUNK_SIZE])
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_err = exc
            if attempt >= retry_max or not is_transient_upstream_error(exc):
                raise
            wait = min(2 ** (attempt - 1), 30)
            logger.warning(
                "Upstream range %d-%d failed (attempt %d/%d): %s; retry in %ss",
                start_pos,
                end_pos,
                attempt,
                retry_max,
                exc,
                wait,
            )
            await asyncio.sleep(wait)

    if last_err is not None:
        raise last_err


async def _get_file_range_from_remote_once(
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
    headers["accept-encoding"] = "identity"

    chunk_bytes = 0
    decompressor: Optional[Decompressor] = None
    async with client.stream(
        method=remote_info.method,
        url=remote_info.url,
        headers=headers,
        timeout=worker_stream_timeout(),
        follow_redirects=True,
    ) as response:
        status_code = response.status_code

        if status_code == 429:
            raise Exception("Too many requests in a given amount of time.")
        if status_code not in (200, 206):
            if is_transient_upstream_status(status_code):
                raise httpx.TransportError(f"Upstream returned HTTP {status_code}")
            body_snippet = (await response.aread())[:256]
            raise Exception(
                f"Upstream returned HTTP {status_code} for range {start_pos}-{end_pos - 1}: {body_snippet!r}"
            )

        is_compressed = "content-encoding" in response.headers
        if is_compressed:
            decompressor = Decompressor(response.headers["content-encoding"].split(","))

        if hasattr(response, "aiter_bytes"):
            chunk_source = response.aiter_bytes(CHUNK_SIZE)
        else:
            chunk_source = response.aiter_raw()

        async for raw_chunk in chunk_source:
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

    if end_pos - start_pos != response_content_length:
        raise Exception(
            f"The content of the response is incomplete. File size: {cache_file._get_file_size()}. Start-end: {start_pos}-{end_pos}. Expected-{end_pos - start_pos}. Accepted-{response_content_length}"
        )


async def _file_chunk_get(
    app,
    save_path: str,
    head_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    allow_cache: bool,
    file_size: int,
    expected_lfs_sha256: Optional[str] = None,
):
    cache_block_size = getattr(
        app.state.app_settings.config, "cache_block_size", LFS_FILE_BLOCK
    )
    cache_lock: Optional[asyncio.Lock] = None
    if allow_cache:
        cache_lock = await _acquire_file_cache_lock(save_path)
    # Redirect Chunks
    if os.path.exists(save_path):
        cache_file = OlahCache(save_path)
    else:
        cache_file = OlahCache.create(save_path, block_size=cache_block_size)
        cache_file.resize(file_size=file_size)
    
    # Refresh access time
    touch_file_access_time(save_path)
    try:
        _, all_ranges, _ = get_request_ranges(file_size, headers.get("range"))

        for start_pos, end_pos in all_ranges:
            ranges_and_cache_list = get_contiguous_ranges(cache_file, start_pos, end_pos)
            # Stream ranges
            for (range_start_pos, range_end_pos), is_remote in ranges_and_cache_list:
                # range_start_pos is zero-index and range_end_pos is exclusive
                block_size = cache_file._get_block_size()
                if is_remote and OLAH_REMOTE_FETCH_BLOCK_ALIGN:
                    fetch_slices = iter_block_bounded_ranges(
                        range_start_pos, range_end_pos, block_size
                    )
                else:
                    fetch_slices = [(range_start_pos, range_end_pos)]

                for sub_start, sub_end in fetch_slices:
                    if is_remote:
                        generator = _get_file_range_from_remote(
                            client,
                            RemoteInfo(method, url, headers),
                            cache_file,
                            sub_start,
                            sub_end,
                        )
                    else:
                        generator = _get_file_range_from_cache(
                            cache_file,
                            sub_start,
                            sub_end,
                            client=client,
                            remote_info=RemoteInfo(method, url, headers),
                        )

                    cur_pos = sub_start
                    stream_cache = bytearray()
                    stream_reset_nonce = cache_file._stream_reset_nonce
                    blocks_written: set = set()
                    last_block, last_block_start_pos, last_block_end_pos = get_block_info(
                        cur_pos, block_size, cache_file._get_file_size()
                    )
                    cur_block = last_block
                    try:
                        async for chunk in generator:
                            if cache_file._stream_reset_nonce != stream_reset_nonce:
                                stream_reset_nonce = cache_file._stream_reset_nonce
                                stream_cache = bytearray()
                                cur_pos = sub_start
                                blocks_written.clear()
                                last_block, last_block_start_pos, last_block_end_pos = get_block_info(
                                    cur_pos,
                                    block_size,
                                    cache_file._get_file_size(),
                                )
                                cur_block = last_block

                            if len(chunk) != 0:
                                yield bytes(chunk)
                                stream_cache += chunk
                                cur_pos += len(chunk)

                            cur_block = cur_pos // block_size

                            if cur_block == last_block:
                                continue
                            split_pos = last_block_end_pos - max(
                                last_block_start_pos, sub_start
                            )
                            raw_block = stream_cache[:split_pos]
                            stream_cache = stream_cache[split_pos:]
                            if len(raw_block) == block_size:
                                await _write_block_safely(
                                    cache_file,
                                    last_block,
                                    raw_block,
                                    allow_cache,
                                    blocks_written,
                                )
                            last_block, last_block_start_pos, last_block_end_pos = get_block_info(
                                cur_pos, block_size, cache_file._get_file_size()
                            )
                    finally:
                        raw_block = bytes(stream_cache)
                        if cur_pos == sub_end:
                            final_block = last_block
                            final_start = last_block_start_pos
                            final_end = last_block_end_pos
                            if final_start >= sub_start:
                                expected_len = final_end - final_start
                                if len(raw_block) == expected_len and _should_persist_block(
                                    cache_file, final_block, len(raw_block)
                                ):
                                    await _write_block_safely(
                                        cache_file,
                                        final_block,
                                        raw_block,
                                        allow_cache,
                                        blocks_written,
                                    )

                    if cur_pos != sub_end:
                        if is_remote:
                            for block_index in blocks_written:
                                cache_file._invalidate_block(
                                    block_index,
                                    "incomplete remote segment at end of stream",
                                )
                            raise Exception(
                                f"The size of remote range ({sub_end - sub_start}) is different from sent size ({cur_pos - sub_start})."
                            )
                        else:
                            raise Exception(
                                f"The size of cached range ({sub_end - sub_start}) is different from sent size ({cur_pos - sub_start})."
                            )

        if (
            allow_cache
            and expected_lfs_sha256
            and OLAH_CACHE_SHA256_VERIFY
        ):
            await verify_cache_sha256(
                cache_file,
                expected_lfs_sha256,
                save_path=save_path,
            )
    finally:
        cache_file.close()
        if cache_lock is not None:
            cache_lock.release()


async def _stream_single_range(
    app,
    save_path: str,
    head_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    allow_cache: bool,
    file_size: int,
    requested_range: Optional[Tuple[int, int]] = None,
    expected_lfs_sha256: Optional[str] = None,
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
        head_path=head_path,
        client=client,
        method=method,
        url=url,
        headers=range_headers,
        allow_cache=allow_cache,
        file_size=file_size,
        expected_lfs_sha256=expected_lfs_sha256,
    ):
        yield chunk


async def _file_chunk_head(
    app,
    save_path: str,
    head_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    allow_cache: bool,
    file_size: int,
):
    if app.state.app_settings.config.offline:
        yield b""
        return

    retry_max = int(os.getenv("OLAH_REMOTE_RETRY_MAX", str(OLAH_REMOTE_RETRY_MAX)))
    last_err: Optional[BaseException] = None
    for attempt in range(1, retry_max + 1):
        try:
            async with client.stream(
                method=method,
                url=url,
                headers=headers,
                timeout=worker_api_timeout(),
            ) as response:
                async for raw_chunk in (
                    response.aiter_bytes(CHUNK_SIZE)
                    if hasattr(response, "aiter_bytes")
                    else response.aiter_raw()
                ):
                    if not raw_chunk:
                        continue
                    yield raw_chunk
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_err = exc
            if attempt >= retry_max or not is_transient_upstream_error(exc):
                raise
            wait = min(2 ** (attempt - 1), 30)
            logger.warning(
                "Upstream HEAD %s failed (attempt %d/%d): %s; retry in %ss",
                url,
                attempt,
                retry_max,
                exc,
                wait,
            )
            await asyncio.sleep(wait)

    if last_err is not None:
        raise last_err


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
                    timeout=worker_api_timeout(),
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
) -> Optional[RemoteFileMetadata]:
    if offline:
        etag = await _resource_etag(hf_url=hf_url, authorization=authorization, offline=True)
        return RemoteFileMetadata(file_size=0, etag=etag)

    headers = {}
    if authorization is not None:
        headers["authorization"] = authorization
    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method="HEAD",
                url=hf_url,
                headers=headers,
                timeout=worker_api_timeout(),
                follow_redirects=True,
            )
    except (httpx.HTTPError, ValueError):
        return None
    if response.status_code >= 400:
        return None

    content_length = response.headers.get("content-length")
    if content_length is None:
        return None
    try:
        file_size = int(content_length)
    except ValueError:
        return None
    return RemoteFileMetadata(file_size=file_size, etag=response.headers.get("etag"))

async def _file_realtime_stream(
    app,
    save_path: str,
    head_path: str,
    url: str,
    request: Request,
    repo_type: Optional[Literal["models", "datasets", "spaces"]] = None,
    org: Optional[str] = None,
    repo: Optional[str] = None,
    file_path: Optional[str] = None,
    method="GET",
    allow_cache=True,
    commit: Optional[str] = None,
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
    expected_lfs_sha256: Optional[str] = None
    if repo_type is not None and org is not None and repo is not None and file_path is not None and commit is not None:
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
        if OLAH_CACHE_SHA256_VERIFY:
            expected_lfs_sha256 = lfs_sha256_from_pathsinfo(pathinfo)
        etag = await _resource_etag(
            hf_url=hf_url,
            authorization=authorization,
            offline=app.state.app_settings.config.offline,
        )
    else:
        metadata = await _remote_file_metadata(
            app=app,
            hf_url=hf_url,
            authorization=authorization,
            offline=app.state.app_settings.config.offline,
        )
        if metadata is None:
            return await error_result(error_proxy_timeout())
        file_size = metadata.file_size
        etag = metadata.etag

    response_headers = {}
    range_header = request_headers.get("range")
    _, all_ranges, _ = get_request_ranges(file_size, range_header)
    response_headers["accept-ranges"] = "bytes"
    # Commit info
    if commit is not None:
        response_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
    response_headers["etag"] = etag
    
    if etag is None:
        return await error_result(error_proxy_timeout())

    if range_header is None:
        status_code = 200
        response_headers["content-length"] = str(file_size)
    elif len(all_ranges) == 0:
        if method.lower() == "get":
            # Stale resume offsets from download clients should not hard-fail; serve the full file.
            range_header = None
            status_code = 200
            response_headers["content-length"] = str(file_size)
            response_headers.pop("content-range", None)
        else:
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

    async def body_iter() -> AsyncIterator[bytes]:
        async with create_stream_client() as client:
            if method.lower() == "get":
                if range_header is None:
                    async for each_chunk in _stream_single_range(
                        app=app,
                        save_path=save_path,
                        head_path=head_path,
                        client=client,
                        method=method,
                        url=hf_url,
                        headers=request_headers,
                        allow_cache=allow_cache,
                        file_size=file_size,
                        expected_lfs_sha256=expected_lfs_sha256,
                    ):
                        yield each_chunk
                elif len(all_ranges) == 1:
                    async for each_chunk in _stream_single_range(
                        app=app,
                        save_path=save_path,
                        head_path=head_path,
                        client=client,
                        method=method,
                        url=hf_url,
                        headers=request_headers,
                        allow_cache=allow_cache,
                        file_size=file_size,
                        requested_range=all_ranges[0],
                        expected_lfs_sha256=expected_lfs_sha256,
                    ):
                        yield each_chunk
                else:
                    for start_pos, end_pos in all_ranges:
                        yield _multipart_part_header(boundary, start_pos, end_pos, file_size)
                        async for each_chunk in _stream_single_range(
                            app=app,
                            save_path=save_path,
                            head_path=head_path,
                            client=client,
                            method=method,
                            url=hf_url,
                            headers=request_headers,
                            allow_cache=allow_cache,
                            file_size=file_size,
                            requested_range=(start_pos, end_pos),
                            expected_lfs_sha256=expected_lfs_sha256,
                        ):
                            yield each_chunk
                        yield b"\r\n"
                    yield f"--{boundary}--\r\n".encode("ascii")
            elif method.lower() == "head":
                async for each_chunk in _file_chunk_head(
                    app=app,
                    save_path=save_path,
                    head_path=head_path,
                    client=client,
                    method=method,
                    url=hf_url,
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
    head_path = os.path.join(
        repos_path, f"heads/{repo_type}/{org_repo}/resolve/{commit}/{file_path}"
    )
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org_repo}/resolve/{commit}/{file_path}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # use_cache = os.path.exists(head_path) and os.path.exists(save_path)
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
        head_path=head_path,
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
    head_path = os.path.join(
        repos_path, f"heads/{repo_type}/{org_repo}/cdn/{file_hash}"
    )
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org_repo}/cdn/{file_hash}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # use_cache = os.path.exists(head_path) and os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    # request_url = urlparse(str(request.url))
    # if request_url.netloc == app.state.app_settings.config.hf_lfs_netloc:
    #     redirected_url = urljoin(app.state.app_settings.config.mirror_lfs_url_base(), get_url_tail(request_url))
    # else:
    #     redirected_url = urljoin(app.state.app_settings.config.mirror_url_base(), get_url_tail(request_url))

    return await _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=str(request.url),
        request=request,
        method=method,
        allow_cache=allow_cache,
    )
