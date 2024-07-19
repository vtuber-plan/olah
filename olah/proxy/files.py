# coding=utf-8
# Copyright 2024 XiaHan
# 
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import hashlib
import json
import os
from typing import Dict, Literal, Optional, Tuple
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
)
from olah.utils.olah_cache import OlahCache
from olah.utils.url_utils import RemoteInfo, check_cache_rules_hf, get_org_repo, get_url_tail, parse_range_params
from olah.utils.file_utils import make_dirs
from olah.constants import CHUNK_SIZE, LFS_FILE_BLOCK, WORKER_API_TIMEOUT

async def _write_cache_request(head_path: str, status_code: int, headers: Dict[str, str], content: bytes):
    rq = {
        "status_code": status_code,
        "headers": headers,
        "content": content.hex(),
    }
    with open(head_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(rq, ensure_ascii=False))

async def _read_cache_request(head_path: str):
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
            response_headers_dict = {k.lower():v for k, v in cache_rq["headers"].items()}
            if "location" in response_headers_dict:
                parsed_url = urlparse(response_headers_dict["location"])
                if len(parsed_url.netloc) != 0:
                    new_loc = urljoin(app.app_settings.config.mirror_lfs_url_base(), get_url_tail(response_headers_dict["location"]))
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
                    await _write_cache_request(head_path, response.status_code, response_headers_dict, response.content)
                elif response.status_code >= 300 and response.status_code <= 399:
                    await _write_cache_request(head_path, response.status_code, response_headers_dict, response.content)
                    from_url = urlparse(url)
                    parsed_url = urlparse(response.headers["location"])
                    if len(parsed_url.netloc) != 0:
                        new_loc = urljoin(app.app_settings.config.mirror_lfs_url_base(), get_url_tail(response.headers["location"]))
                        response_headers_dict["location"] = new_loc
                else:
                    raise Exception(f"Unexpected HTTP status code {response.status_code}")
            return response.status_code, response_headers_dict, response.content
    else:
        if os.path.exists(head_path):
            cache_rq = await _read_cache_request(head_path)
            response_headers_dict = {k.lower():v for k, v in cache_rq["headers"].items()}
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
            new_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = response_headers_dict.get(HUGGINGFACE_HEADER_X_REPO_COMMIT.lower(), "")
        if HUGGINGFACE_HEADER_X_LINKED_ETAG.lower() in response_headers_dict:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_ETAG.lower()] = response_headers_dict.get(HUGGINGFACE_HEADER_X_LINKED_ETAG.lower(), "")
        if HUGGINGFACE_HEADER_X_LINKED_SIZE.lower() in response_headers_dict:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_SIZE.lower()] = response_headers_dict.get(HUGGINGFACE_HEADER_X_LINKED_SIZE.lower(), "")
        if "etag" in response_headers_dict:
            new_headers["etag"] = response_headers_dict["etag"]
        if "location" in response_headers_dict:
            new_headers["location"] = urljoin(app.app_settings.config.mirror_lfs_url_base(), get_url_tail(response_headers_dict["location"]))
        return cache_rq["status_code"], new_headers, cache_rq["content"]

async def _get_file_block_from_cache(cache_file: OlahCache, block_index: int):
    raw_block = cache_file.read_block(block_index)
    return raw_block

async def _get_file_block_from_remote(client: httpx.AsyncClient, remote_info: RemoteInfo, cache_file: OlahCache, block_index: int):
    block_start_pos = block_index * cache_file._get_block_size()
    block_end_pos = min(
        (block_index + 1) * cache_file._get_block_size(), cache_file._get_file_size()
    )
    remote_info.headers["range"] = f"bytes={block_start_pos}-{block_end_pos - 1}"
    raw_block = bytearray()
    async with client.stream(
        method=remote_info.method,
        url=remote_info.url,
        headers=remote_info.headers,
        timeout=WORKER_API_TIMEOUT,
    ) as response:
        response_content_length = int(response.headers['content-length'])
        async for raw_chunk in response.aiter_raw():
            if not raw_chunk:
                continue
            raw_block += raw_chunk
    # print(remote_info.url, remote_info.method, remote_info.headers)
    # print(block_start_pos, block_end_pos)
    if len(raw_block) != response_content_length:
        raise Exception(f"The content of the response is incomplete. Expected-{response_content_length}. Accepted-{len(raw_block)}")
    if len(raw_block) != (block_end_pos - block_start_pos):
        raise Exception(f"The block is incomplete. Expected-{block_end_pos - block_start_pos}. Accepted-{len(raw_block)}")
    if len(raw_block) < cache_file._get_block_size():
        raw_block += b"\x00" * (cache_file._get_block_size() - len(raw_block))
    return raw_block

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
        start_pos, end_pos = parse_range_params(headers.get("range", f"bytes={0}-{file_size}"), file_size)

        start_block = start_pos // cache_file._get_block_size()
        end_block = end_pos // cache_file._get_block_size()

        cur_pos = start_pos
        cur_block = start_block

        while cur_block <= end_block:
            block_start_pos = cur_block * cache_file._get_block_size()
            block_end_pos = min(
                (cur_block + 1) * cache_file._get_block_size(), file_size
            )
            if cache_file.has_block(cur_block):
                raw_block = await _get_file_block_from_cache(
                    cache_file, cur_block,
                )
            else:
                raw_block = await _get_file_block_from_remote(
                    client,
                    RemoteInfo(method, url, headers),
                    cache_file,
                    cur_block,
                )

            s = cur_pos - block_start_pos
            e = block_end_pos - block_start_pos
            chunk = raw_block[s:e]
            if len(chunk) != 0:
                yield bytes(chunk)
            cur_pos += len(chunk)

            if len(raw_block) != cache_file._get_block_size():
                raise Exception(f"The size of raw block {len(raw_block)} is different from blocksize {cache_file._get_block_size()}.")
            if not cache_file.has_block(cur_block) and allow_cache:
                cache_file.write_block(cur_block, raw_block)

            cur_block += 1
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
    request_headers = {k: v for k, v in request.headers.items()}
    request_headers.pop("host")

    if urlparse(url).netloc == app.app_settings.config.mirror_netloc:
        hf_url = urljoin(app.app_settings.config.hf_lfs_url_base(), get_url_tail(url))
    else:
        hf_url = url

    async with httpx.AsyncClient() as client:
        # redirect_loc = await _get_redirected_url(client, method, url, request_headers)
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

        file_size = int(head_info["content-length"])
        response_headers = {k: v for k,v in head_info.items()}
        if "range" in request_headers:
            start_pos, end_pos = parse_range_params(request_headers.get("range", f"bytes={0}-{file_size}"), file_size)
            response_headers["content-length"] = str(end_pos - start_pos)
        if commit is not None:
            response_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit

        if app.app_settings.config.offline and "etag" not in response_headers:
            # Create fake headers when offline mode
            sha256_hash = hashlib.sha256()
            sha256_hash.update(hf_url.encode('utf-8'))
            content_hash = sha256_hash.hexdigest()
            response_headers["etag"] = f"\"{content_hash[:32]}-10\""
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
        url = urljoin(app.app_settings.config.hf_url_base(), f"/{org_repo}/resolve/{commit}/{file_path}")
    else:
        url = urljoin(app.app_settings.config.hf_url_base(), f"/{repo_type}/{org_repo}/resolve/{commit}/{file_path}")
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
