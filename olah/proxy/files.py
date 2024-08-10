# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import hashlib
import json
import os
from typing import Dict, List, Literal, Optional, Tuple
from fastapi import Request

from requests.structures import CaseInsensitiveDict
import httpx
from starlette.datastructures import URL
from urllib.parse import urlparse, urljoin

from olah.constants import (
    CHUNK_SIZE,
    WORKER_API_TIMEOUT,
    HUGGINGFACE_HEADER_X_REPO_COMMIT,
    HUGGINGFACE_HEADER_X_LINKED_ETAG,
    HUGGINGFACE_HEADER_X_LINKED_SIZE,
    ORIGINAL_LOC,
)
from olah.utils.olah_cache import OlahCache
from olah.utils.url_utils import (
    RemoteInfo,
    add_query_param,
    check_url_has_param_name,
    get_url_param_name,
    get_url_tail,
    parse_range_params,
    remove_query_param,
)
from olah.utils.repo_utils import get_org_repo
from olah.utils.rule_utils import check_cache_rules_hf
from olah.utils.file_utils import make_dirs
from olah.constants import CHUNK_SIZE, LFS_FILE_BLOCK, WORKER_API_TIMEOUT


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


async def _write_cache_request(
    head_path: str, status_code: int, headers: Dict[str, str], content: bytes
) -> None:
    """
    Write the request's status code, headers, and content to a cache file.

    Args:
        head_path (str): The path to the cache file.
        status_code (int): The status code of the request.
        headers (Dict[str, str]): The dictionary of response headers.
        content (bytes): The content of the request.

    Returns:
        None
    """
    rq = {
        "status_code": status_code,
        "headers": headers,
        "content": content.hex(),
    }
    with open(head_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(rq, ensure_ascii=False))


async def _read_cache_request(head_path: str) -> Dict[str, str]:
    """
    Read the request's status code, headers, and content from a cache file.

    Args:
        head_path (str): The path to the cache file.

    Returns:
        Dict[str, str]: A dictionary containing the status code, headers, and content of the request.
    """
    with open(head_path, "r", encoding="utf-8") as f:
        rq = json.loads(f.read())

    rq["content"] = bytes.fromhex(rq["content"])
    return rq


async def _file_full_header(
    app,
    save_path: str,
    head_path: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    allow_cache: bool,
) -> Tuple[int, Dict[str, str], bytes]:
    assert method.lower() == "head"
    if not app.app_settings.config.offline:
        if os.path.exists(head_path):
            cache_rq = await _read_cache_request(head_path)
            response_headers_dict = {
                k.lower(): v for k, v in cache_rq["headers"].items()
            }
            if "location" in response_headers_dict:
                parsed_url = urlparse(response_headers_dict["location"])
                if len(parsed_url.netloc) != 0:
                    new_loc = urljoin(
                        app.app_settings.config.mirror_lfs_url_base(),
                        get_url_tail(response_headers_dict["location"]),
                    )
                    response_headers_dict["location"] = new_loc
            return cache_rq["status_code"], response_headers_dict, cache_rq["content"]
        else:
            if "range" in headers:
                headers.pop("range")
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
            )
            response_headers_dict = {k.lower(): v for k, v in response.headers.items()}
            if allow_cache and method.lower() == "head":
                if response.status_code == 200:
                    await _write_cache_request(
                        head_path,
                        response.status_code,
                        response_headers_dict,
                        response.content,
                    )
                elif response.status_code >= 300 and response.status_code <= 399:
                    from_url = urlparse(url)
                    parsed_url = urlparse(response.headers["location"])
                    if len(parsed_url.netloc) != 0:
                        new_loc = urljoin(
                            app.app_settings.config.mirror_lfs_url_base(),
                            get_url_tail(response.headers["location"]),
                        )
                        response_headers_dict["location"] = new_loc
                    # Redirect, add original location info
                    if check_url_has_param_name(
                        response_headers_dict["location"], ORIGINAL_LOC
                    ):
                        raise Exception(f"Invalid field {ORIGINAL_LOC} in the url.")
                    else:
                        response_headers_dict["location"] = add_query_param(
                            response_headers_dict["location"],
                            ORIGINAL_LOC,
                            response.headers["location"],
                        )
                    await _write_cache_request(
                        head_path,
                        response.status_code,
                        response_headers_dict,
                        response.content,
                    )
                elif response.status_code == 403:
                    pass
                else:
                    raise Exception(
                        f"Unexpected HTTP status code {response.status_code}"
                    )
            return response.status_code, response_headers_dict, response.content
    else:
        if os.path.exists(head_path):
            cache_rq = await _read_cache_request(head_path)
            response_headers_dict = {
                k.lower(): v for k, v in cache_rq["headers"].items()
            }
        else:
            response_headers_dict = {}
            cache_rq = {
                "status_code": 200,
                "headers": response_headers_dict,
                "content": b"",
            }

        new_headers = {}
        if "content-type" in response_headers_dict:
            new_headers["content-type"] = response_headers_dict["content-type"]
        if "content-length" in response_headers_dict:
            new_headers["content-length"] = response_headers_dict["content-length"]
        if HUGGINGFACE_HEADER_X_REPO_COMMIT.lower() in response_headers_dict:
            new_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = (
                response_headers_dict.get(HUGGINGFACE_HEADER_X_REPO_COMMIT.lower(), "")
            )
        if HUGGINGFACE_HEADER_X_LINKED_ETAG.lower() in response_headers_dict:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_ETAG.lower()] = (
                response_headers_dict.get(HUGGINGFACE_HEADER_X_LINKED_ETAG.lower(), "")
            )
        if HUGGINGFACE_HEADER_X_LINKED_SIZE.lower() in response_headers_dict:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_SIZE.lower()] = (
                response_headers_dict.get(HUGGINGFACE_HEADER_X_LINKED_SIZE.lower(), "")
            )
        if "etag" in response_headers_dict:
            new_headers["etag"] = response_headers_dict["etag"]
        if "location" in response_headers_dict:
            new_headers["location"] = urljoin(
                app.app_settings.config.mirror_lfs_url_base(),
                get_url_tail(response_headers_dict["location"]),
            )
        return cache_rq["status_code"], new_headers, cache_rq["content"]


async def _get_file_range_from_cache(
    cache_file: OlahCache, start_pos: int, end_pos: int
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
        raw_block = cache_file.read_block(cur_block)
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
    remote_info.headers["range"] = f"bytes={start_pos}-{end_pos - 1}"
    chunk_bytes = 0
    async with client.stream(
        method=remote_info.method,
        url=remote_info.url,
        headers=remote_info.headers,
        timeout=WORKER_API_TIMEOUT,
    ) as response:
        response_content_length = int(response.headers["content-length"])
        async for raw_chunk in response.aiter_raw():
            if not raw_chunk:
                continue
            yield raw_chunk
            chunk_bytes += len(raw_chunk)

    if end_pos - start_pos != response_content_length:
        raise Exception(
            f"The content of the response is incomplete. Expected-{end_pos - start_pos}. Accepted-{response_content_length}"
        )
    if end_pos - start_pos != chunk_bytes:
        raise Exception(
            f"The block is incomplete. Expected-{end_pos - start_pos}. Accepted-{chunk_bytes}"
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
):
    # Redirect Chunks
    if os.path.exists(save_path):
        cache_file = OlahCache(save_path)
    else:
        cache_file = OlahCache.create(save_path)
        cache_file.resize(file_size=file_size)
    try:
        start_pos, end_pos = parse_range_params(
            headers.get("range", f"bytes={0}-{file_size-1}"), file_size
        )
        end_pos += 1

        ranges_and_cache_list = get_contiguous_ranges(cache_file, start_pos, end_pos)
        # Stream ranges
        for (range_start_pos, range_end_pos), is_remote in ranges_and_cache_list:
            if is_remote:
                generator = _get_file_range_from_remote(
                    client,
                    RemoteInfo(method, url, headers),
                    cache_file,
                    range_start_pos,
                    range_end_pos,
                )
            else:
                generator = _get_file_range_from_cache(
                    cache_file,
                    range_start_pos,
                    range_end_pos,
                )

            cur_pos = range_start_pos
            stream_cache = bytearray()
            last_block, last_block_start_pos, last_block_end_pos = get_block_info(
                cur_pos, cache_file._get_block_size(), cache_file._get_file_size()
            )
            async for chunk in generator:
                if len(chunk) != 0:
                    yield bytes(chunk)
                    stream_cache += chunk
                    cur_pos += len(chunk)

                cur_block = cur_pos // cache_file._get_block_size()

                if cur_block == last_block:
                    continue
                split_pos = last_block_end_pos - max(
                    last_block_start_pos, range_start_pos
                )
                raw_block = stream_cache[:split_pos]
                stream_cache = stream_cache[split_pos:]
                if len(raw_block) == cache_file._get_block_size():
                    if not cache_file.has_block(last_block) and allow_cache:
                        cache_file.write_block(last_block, raw_block)
                last_block, last_block_start_pos, last_block_end_pos = get_block_info(
                    cur_pos, cache_file._get_block_size(), cache_file._get_file_size()
                )

            raw_block = stream_cache
            if cur_block == cache_file._get_block_number() - 1:
                if (
                    len(raw_block)
                    == cache_file._get_file_size() % cache_file._get_block_size()
                ):
                    raw_block += b"\x00" * (
                        cache_file._get_block_size() - len(raw_block)
                    )
                last_block = cur_block
            if len(raw_block) == cache_file._get_block_size():
                if not cache_file.has_block(last_block) and allow_cache:
                    cache_file.write_block(last_block, raw_block)

            if cur_pos != range_end_pos:
                if is_remote:
                    raise Exception(
                        f"The size of remote range ({range_end_pos - range_start_pos}) is different from sent size ({cur_pos - range_start_pos})."
                    )
                else:
                    raise Exception(
                        f"The size of cached range ({range_end_pos - range_start_pos}) is different from sent size ({cur_pos - range_start_pos})."
                    )
    finally:
        cache_file.close()


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
    if not app.app_settings.config.offline:
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


async def _file_realtime_stream(
    app,
    save_path: str,
    head_path: str,
    url: str,
    request: Request,
    method="GET",
    allow_cache=True,
    commit: Optional[str] = None,
):
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
                app.app_settings.config.hf_lfs_url_base(), get_url_tail(clean_url)
            )
    else:
        if urlparse(url).netloc in [
            app.app_settings.config.hf_netloc,
            app.app_settings.config.hf_lfs_netloc,
        ]:
            hf_url = url
        else:
            hf_url = urljoin(
                app.app_settings.config.hf_lfs_url_base(), get_url_tail(url)
            )

    request_headers = {k: v for k, v in request.headers.items()}
    if "host" in request_headers:
        request_headers["host"] = urlparse(hf_url).netloc

    async with httpx.AsyncClient() as client:
        # redirect_loc = await _get_redirected_url(client, method, url, request_headers)
        try:
            status_code, head_info, content = await _file_full_header(
                app=app,
                save_path=save_path,
                head_path=head_path,
                client=client,
                method="HEAD",
                url=hf_url,
                headers=request_headers,
                allow_cache=allow_cache,
            )
            if status_code != 200:
                yield status_code
                yield head_info
                yield content
                return
        except httpx.ConnectError:
            yield 504
            yield {}
            yield b""
            return

    async with httpx.AsyncClient() as client:
        file_size = int(head_info["content-length"])
        response_headers = {k: v for k, v in head_info.items()}
        if "range" in request_headers:
            start_pos, end_pos = parse_range_params(
                request_headers.get("range", f"bytes={0}-{file_size-1}"), file_size
            )
            response_headers["content-length"] = str(end_pos - start_pos + 1)
        if commit is not None:
            response_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit

        if app.app_settings.config.offline and "etag" not in response_headers:
            # Create fake headers when offline mode
            sha256_hash = hashlib.sha256()
            sha256_hash.update(hf_url.encode("utf-8"))
            content_hash = sha256_hash.hexdigest()
            response_headers["etag"] = f'"{content_hash[:32]}-10"'
        yield 200
        yield response_headers
        if method.lower() == "get":
            async for each_chunk in _file_chunk_get(
                app=app,
                save_path=save_path,
                head_path=head_path,
                client=client,
                method=method,
                url=hf_url,
                headers=request_headers,
                allow_cache=allow_cache,
                file_size=file_size,
            ):
                yield each_chunk
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
    repos_path = app.app_settings.repos_path
    head_path = os.path.join(
        repos_path, f"heads/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path}"
    )
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # use_cache = os.path.exists(head_path) and os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    if repo_type == "models":
        url = urljoin(
            app.app_settings.config.hf_url_base(),
            f"/{org_repo}/resolve/{commit}/{file_path}",
        )
    else:
        url = urljoin(
            app.app_settings.config.hf_url_base(),
            f"/{repo_type}/{org_repo}/resolve/{commit}/{file_path}",
        )
    return _file_realtime_stream(
        app=app,
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
    repos_path = app.app_settings.repos_path
    head_path = os.path.join(
        repos_path, f"heads/{repo_type}/{org}/{repo}/cdn/{file_hash}"
    )
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org}/{repo}/cdn/{file_hash}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # use_cache = os.path.exists(head_path) and os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    # request_url = urlparse(str(request.url))
    # if request_url.netloc == app.app_settings.config.hf_lfs_netloc:
    #     redirected_url = urljoin(app.app_settings.config.mirror_lfs_url_base(), get_url_tail(request_url))
    # else:
    #     redirected_url = urljoin(app.app_settings.config.mirror_url_base(), get_url_tail(request_url))

    return _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=str(request.url),
        request=request,
        method=method,
        allow_cache=allow_cache,
    )
