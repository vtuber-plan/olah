import json
import os
import shutil
import tempfile
from typing import Literal, Optional
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
from olah.utils import check_cache_rules_hf, get_org_repo, get_url_tail, make_dirs
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

async def _get_redirected_url(client, method: str, url, headers):
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

async def _file_realtime_stream(
    app, save_path: str, head_path: str, url: str, request: Request, method="GET", allow_cache=True, commit: Optional[str]=None
):
    request_headers = {k: v for k, v in request.headers.items()}
    request_headers.pop("host")
    temp_file_path = None
    try:
        async with httpx.AsyncClient() as client:
            with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
                temp_file_path = temp_file.name

                if not allow_cache or request.method.lower() == "head":
                    write_temp_file = False
                else:
                    write_temp_file = True
                
                redirect_loc = await _get_redirected_url(client, method, url, request_headers)
                async with client.stream(
                    method=method,
                    url=redirect_loc,
                    headers=request_headers,
                    timeout=WORKER_API_TIMEOUT,
                ) as response:
                    response_headers_dict = {k.lower(): v for k, v in response.headers.items()}
                    if allow_cache:
                        if request.method.lower() == "head":
                            with open(head_path, "w", encoding="utf-8") as f:
                                f.write(json.dumps(response_headers_dict, ensure_ascii=False))
                    if "location" in response_headers_dict:
                        location_url = urlparse(response_headers_dict["location"])
                        if location_url.netloc == app.app_settings.config.hf_lfs_netloc:
                            response_headers_dict["location"] = urljoin(app.app_settings.config.mirror_lfs_url_base(), get_url_tail(location_url))
                        else:
                            response_headers_dict["location"] = urljoin(app.app_settings.config.mirror_url_base(), get_url_tail(location_url))
                    if commit is not None:
                        response_headers_dict[HUGGINGFACE_HEADER_X_REPO_COMMIT.lower()] = commit
                    yield response_headers_dict

                    async for raw_chunk in response.aiter_raw():
                        if not raw_chunk:
                            continue
                        if write_temp_file:
                            temp_file.write(raw_chunk)
                        yield raw_chunk

                if temp_file_path is not None and write_temp_file:
                    temp_file.flush()
                    shutil.copyfile(temp_file_path, save_path)
    finally:
        if temp_file_path is not None and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

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
    if use_cache:
        return _file_cache_stream(save_path=save_path, head_path=head_path, request=request)
    else:
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
    if use_cache:
        return _file_cache_stream(save_path=save_path, head_path=head_path, request=request)
    else:
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
    if use_cache:
        return _file_cache_stream(save_path=save_path, request=request)
    else:
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
