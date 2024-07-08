import json
import os
import shutil
import tempfile
from typing import Literal
from fastapi import Request

import httpx
from starlette.datastructures import URL

from olah.constants import CHUNK_SIZE, WORKER_API_TIMEOUT
from olah.utls import check_cache_rules_hf, get_org_repo


async def _file_head_cache_stream(app, save_path: str, request: Request):
    with open(save_path, "r", encoding="utf-8") as f:
        response_headers = json.loads(f.read())
        if "location" in response_headers:
            response_headers["location"] = response_headers["location"].replace(
                app.app_settings.hf_url, app.app_settings.mirror_url
            )
        yield response_headers


async def _file_head_realtime_stream(
    app,
    save_path: str,
    url: str,
    headers,
    request: Request,
    method="HEAD",
    allow_cache=True,
):
    async with httpx.AsyncClient() as client:
        async with client.stream(
            method=method,
            url=url,
            headers=headers,
            timeout=WORKER_API_TIMEOUT,
        ) as response:
            response_headers = response.headers
            response_headers = {k: v for k, v in response_headers.items()}
            if allow_cache:
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(response_headers, ensure_ascii=False))
            if "location" in response_headers:
                response_headers["location"] = response_headers["location"].replace(
                    app.app_settings.hf_url, app.app_settings.mirror_url
                )
            yield response_headers

            async for raw_chunk in response.aiter_raw():
                if not raw_chunk:
                    continue
                yield raw_chunk


async def file_head_generator(
    app,
    repo_type: Literal["models", "datasets"],
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    request: Request,
):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_path = os.path.join(
        repos_path, f"heads/{repo_type}/{org}/{repo}/resolve_head/{commit}/{file_path}"
    )
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    if use_cache:
        return _file_head_cache_stream(app=app, save_path=save_path, request=request)
    else:
        if repo_type == "models":
            url = f"{app.app_settings.hf_url}/{org}/{repo}/resolve/{commit}/{file_path}"
        else:
            url = f"{app.app_settings.hf_url}/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path}"
        return _file_head_realtime_stream(
            app=app,
            save_path=save_path,
            url=url,
            headers=headers,
            request=request,
            method="HEAD",
            allow_cache=allow_cache,
        )


async def _file_cache_stream(save_path: str, request: Request):
    yield request.headers
    with open(save_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


async def _file_realtime_stream(
    save_path: str, url: str, headers, request: Request, method="GET", allow_cache=True
):
    temp_file_path = None
    try:
        async with httpx.AsyncClient() as client:
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                if not allow_cache:
                    temp_file = open(os.devnull, "wb")
                async with client.stream(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=WORKER_API_TIMEOUT,
                ) as response:
                    response_headers = response.headers
                    yield response_headers

                    async for raw_chunk in response.aiter_raw():
                        if not raw_chunk:
                            continue
                        temp_file.write(raw_chunk)
                        yield raw_chunk
                if not allow_cache:
                    temp_file_path = None
                else:
                    temp_file_path = temp_file.name
            if temp_file_path is not None:
                shutil.copyfile(temp_file_path, save_path)
    finally:
        if temp_file_path is not None:
            os.remove(temp_file_path)


async def file_get_generator(
    app,
    repo_type: Literal["models", "datasets"],
    org: str,
    repo: str,
    commit: str,
    file_path: str,
    request: Request,
):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")
    # save
    repos_path = app.app_settings.repos_path
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path}"
    )
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    if use_cache:
        return _file_cache_stream(save_path=save_path, request=request)
    else:
        if repo_type == "models":
            url = f"{app.app_settings.hf_url}/{org}/{repo}/resolve/{commit}/{file_path}"
        else:
            url = f"{app.app_settings.hf_url}/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path}"
        return _file_realtime_stream(
            save_path=save_path,
            url=url,
            headers=headers,
            request=request,
            method="GET",
            allow_cache=allow_cache,
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
    save_path = os.path.join(
        repos_path, f"files/{repo_type}/cdn/{org}/{repo}/{file_hash}"
    )
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    # proxy
    if use_cache:
        return _file_cache_stream(save_path=save_path, request=request)
    else:
        redirected_url = str(request.url)
        redirected_url = redirected_url.replace(app.app_settings.hf_lfs_url, app.app_settings.mirror_lfs_url)
 
        return _file_realtime_stream(
            save_path=save_path,
            url=str(redirected_url),
            headers=headers,
            request=request,
            method="GET",
            allow_cache=allow_cache,
        )
