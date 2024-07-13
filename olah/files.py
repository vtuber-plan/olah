import json
import os
import shutil
import tempfile
from typing import Dict, Literal, Optional
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

FILE_HEADER_TEMPLATE = {
    "accept-ranges": "bytes",
    "access-control-allow-origin": "*",
    "cache-control": "public, max-age=604800, immutable, s-maxage=604800",
    # "content-length": None,
    # "content-type": "binary/octet-stream",
    # "etag": None,
    # "last-modified": None,
}

async def _file_cache_stream(save_path: str, head_path: str, request: Request):
    if request.method.lower() == "head":
        with open(head_path, "r", encoding="utf-8") as f:
            response_headers = json.loads(f.read())
        response_headers = {k.lower():v for k, v in response_headers.items()}
        new_headers = {k.lower():v for k, v in FILE_HEADER_TEMPLATE.items()}
        new_headers["content-type"] = response_headers["content-type"]
        new_headers["content-length"] = response_headers["content-length"]
        if HUGGINGFACE_HEADER_X_REPO_COMMIT.lower() in response_headers:
            new_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = response_headers.get(HUGGINGFACE_HEADER_X_REPO_COMMIT.lower(), "")
        if HUGGINGFACE_HEADER_X_LINKED_ETAG.lower() in response_headers:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_ETAG.lower()] = response_headers.get(HUGGINGFACE_HEADER_X_LINKED_ETAG.lower(), "")
        if HUGGINGFACE_HEADER_X_LINKED_SIZE.lower() in response_headers:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_SIZE.lower()] = response_headers.get(HUGGINGFACE_HEADER_X_LINKED_SIZE.lower(), "")
        new_headers["etag"] = response_headers["etag"]
        yield new_headers
    elif request.method.lower() == "get":
        yield FILE_HEADER_TEMPLATE
    else:
        raise Exception(f"Invalid Method type {request.method}")
    with open(save_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

async def _get_redirected_url(client: httpx.AsyncClient, method: str, url: str, headers: Dict[str, str]):
    async with client.stream(
        method=method,
        url=url,
        headers=headers,
        timeout=WORKER_API_TIMEOUT,
    ) as response:
        if response.status_code >= 300 and response.status_code <= 399:
            from_url = urlparse(url)
            parsed_url = urlparse(response.headers["location"])
            if len(parsed_url.netloc) == 0:
                redirect_loc = urljoin(f"{from_url.scheme}://{from_url.netloc}", response.headers["location"])
            else:
                redirect_loc = response.headers["location"]
        else:
            redirect_loc = url
    return redirect_loc

async def _file_full_header(
        app,
        save_path: str,
        head_path: str,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        headers: Dict[str, str],
        allow_cache: bool,
        commit: Optional[str] = None,
    ):
    if os.path.exists(head_path):
        with open(head_path, "r", encoding="utf-8") as f:
            response_headers = json.loads(f.read())
        response_headers_dict = {k.lower():v for k, v in response_headers.items()}
    else:
        if "range" in headers:
            headers.pop("range")
        async with client.stream(
            method=method,
            url=url,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        ) as response:
            response_headers_dict = {k.lower(): v for k, v in response.headers.items()}
            if allow_cache and method.lower() == "head":
                with open(head_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(response_headers_dict, ensure_ascii=False))

    new_headers = {}
    new_headers["content-type"] = response_headers_dict["content-type"]
    new_headers["content-length"] = response_headers_dict["content-length"]
    if HUGGINGFACE_HEADER_X_REPO_COMMIT.lower() in response_headers_dict:
        new_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = response_headers_dict.get(HUGGINGFACE_HEADER_X_REPO_COMMIT.lower(), "")
    if HUGGINGFACE_HEADER_X_LINKED_ETAG.lower() in response_headers_dict:
        new_headers[HUGGINGFACE_HEADER_X_LINKED_ETAG.lower()] = response_headers_dict.get(HUGGINGFACE_HEADER_X_LINKED_ETAG.lower(), "")
    if HUGGINGFACE_HEADER_X_LINKED_SIZE.lower() in response_headers_dict:
        new_headers[HUGGINGFACE_HEADER_X_LINKED_SIZE.lower()] = response_headers_dict.get(HUGGINGFACE_HEADER_X_LINKED_SIZE.lower(), "")
    new_headers["etag"] = response_headers_dict["etag"]
    return new_headers

async def _file_header(
        app,
        save_path: str,
        head_path: str,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        headers: Dict[str, str],
        allow_cache: bool,
        commit: Optional[str] = None,
    ):
    if os.path.exists(head_path):
        with open(head_path, "r", encoding="utf-8") as f:
            response_headers = json.loads(f.read())
        response_headers = {k.lower():v for k, v in response_headers.items()}
        new_headers = {k.lower():v for k, v in FILE_HEADER_TEMPLATE.items()}
        new_headers["content-type"] = response_headers["content-type"]
        # new_headers["content-length"] = response_headers["content-length"]
        if HUGGINGFACE_HEADER_X_REPO_COMMIT.lower() in response_headers:
            new_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = response_headers.get(HUGGINGFACE_HEADER_X_REPO_COMMIT.lower(), "")
        if HUGGINGFACE_HEADER_X_LINKED_ETAG.lower() in response_headers:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_ETAG.lower()] = response_headers.get(HUGGINGFACE_HEADER_X_LINKED_ETAG.lower(), "")
        if HUGGINGFACE_HEADER_X_LINKED_SIZE.lower() in response_headers:
            new_headers[HUGGINGFACE_HEADER_X_LINKED_SIZE.lower()] = response_headers.get(HUGGINGFACE_HEADER_X_LINKED_SIZE.lower(), "")
        new_headers["etag"] = response_headers["etag"]

        if commit is not None:
            new_headers[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
        return new_headers
    else:
        # Redirect Header
        if "range" in headers:
            headers.pop("range")
        async with client.stream(
            method=method,
            url=url,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        ) as response:
            response_headers_dict = {k.lower(): v for k, v in response.headers.items()}
            if allow_cache and method.lower() == "head":
                with open(head_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(response_headers_dict, ensure_ascii=False))
            if "location" in response_headers_dict:
                location_url = urlparse(response_headers_dict["location"])
                if location_url.netloc == app.app_settings.config.hf_lfs_netloc:
                    response_headers_dict["location"] = urljoin(
                        app.app_settings.config.mirror_lfs_url_base(),
                        get_url_tail(location_url),
                    )
                else:
                    response_headers_dict["location"] = urljoin(
                        app.app_settings.config.mirror_url_base(),
                        get_url_tail(location_url),
                    )
            if commit is not None:
                response_headers_dict[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
            return response_headers_dict

async def _get_file_block_from_cache(cache_file: OlahCache, block_index: int):
    return cache_file.read_block(block_index)

async def _get_file_block_from_remote(client, remote_info: RemoteInfo, cache_file: OlahCache, block_index: int):
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
        async for raw_chunk in response.aiter_raw():
            if not raw_chunk:
                continue
            raw_block += raw_chunk
    # print(remote_info.url, remote_info.method, remote_info.headers)
    # print(block_start_pos, block_end_pos)
    if len(raw_block) != (block_end_pos - block_start_pos):
        raise Exception(f"The block is incomplete. Expected-{block_end_pos - block_start_pos}. Accepted-{len(raw_block)}")
    if len(raw_block) < cache_file.header.block_size:
        raw_block += b"\x00" * (cache_file.header.block_size - len(raw_block))
    # print(len(raw_block))
    return bytes(raw_block)

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
    commit: Optional[str] = None,
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
                    cache_file, cur_block
                )
            else:
                raw_block = await _get_file_block_from_remote(
                    client,
                    RemoteInfo(method, url, headers),
                    cache_file,
                    cur_block,
                )
                cache_file.write_block(cur_block, raw_block)
            
            if len(raw_block) != cache_file._get_block_size():
                raise Exception(f"The size of raw block {len(raw_block)} is different from blocksize {cache_file._get_block_size()}.")
            s = cur_pos - block_start_pos
            e = block_end_pos - block_start_pos
            chunk = raw_block[s:e]

            if len(chunk) != 0:
                yield chunk
            cur_pos += len(chunk)
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
    commit: Optional[str] = None,
):
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

    async with httpx.AsyncClient() as client:
        redirect_loc = await _get_redirected_url(client, method, url, request_headers)
        head_info = await _file_full_header(
            app=app,
            save_path=save_path,
            head_path=head_path,
            client=client,
            method=method,
            url=redirect_loc,
            headers=request_headers,
            allow_cache=allow_cache,
            commit=commit,
        )
        response_headers = await _file_header(
            app=app,
            save_path=save_path,
            head_path=head_path,
            client=client,
            method=method,
            url=redirect_loc,
            headers=request_headers,
            allow_cache=allow_cache,
            commit=commit,
        )
        yield response_headers
        if method.lower() == "get":
            async for each_chunk in _file_chunk_get(
                app=app,
                save_path=save_path,
                head_path=head_path,
                client=client,
                method=method,
                url=redirect_loc,
                headers=request_headers,
                allow_cache=allow_cache,
                file_size=int(head_info["content-length"]),
                commit=commit,
            ):
                yield each_chunk
        else:
            async for each_chunk in _file_chunk_head(
                app=app,
                save_path=save_path,
                head_path=head_path,
                client=client,
                method=method,
                url=redirect_loc,
                headers=request_headers,
                allow_cache=allow_cache,
                file_size=0,
                commit=commit,
            ):
                yield each_chunk


async def file_head_generator(
    app,
    repo_type: Literal["models", "datasets"],
    org: str,
    repo: str,
    commit: str,
    file_path: str,
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

    use_cache = os.path.exists(head_path) and os.path.exists(save_path)
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
        method="HEAD",
        allow_cache=allow_cache,
        commit=commit,
    )


async def file_get_generator(
    app,
    repo_type: Literal["models", "datasets"],
    org: str,
    repo: str,
    commit: str,
    file_path: str,
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

    use_cache = os.path.exists(head_path) and os.path.exists(save_path)
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
        method="GET",
        allow_cache=allow_cache,
        commit=commit,
    )


async def cdn_file_get_generator(
    app,
    repo_type: Literal["models", "datasets"],
    org: str,
    repo: str,
    file_hash: str,
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

    use_cache = os.path.exists(head_path) and os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    request_url = urlparse(request.url)
    if request_url.netloc == app.app_settings.config.hf_lfs_netloc:
        redirected_url = urljoin(app.app_settings.config.mirror_lfs_url_base(), get_url_tail(request_url))
    else:
        redirected_url = urljoin(app.app_settings.config.mirror_url_base(), get_url_tail(request_url))

    return _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=redirected_url,
        request=request,
        method="GET",
        allow_cache=allow_cache,
    )
