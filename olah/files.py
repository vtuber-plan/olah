
import json
import os
import shutil
import tempfile
from typing import Literal
from fastapi import Request

import httpx

from olah.constants import CHUNK_SIZE, WORKER_API_TIMEOUT

async def file_head_generator(app, repo_type: Literal["model", "dataset"], org: str, repo: str, commit: str, file_path: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_path = os.path.join(repos_path, f"heads/{repo_type}s/{org}/{repo}/resolve_head/{commit}/{file_path}")
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    
    use_cache = os.path.exists(save_path)

    # proxy
    if use_cache:
        with open(save_path, "r", encoding="utf-8") as f:
            response_headers = json.loads(f.read())
            if "location" in response_headers:
                response_headers["location"] = response_headers["location"].replace(app.app_settings.hf_lfs_url, app.app_settings.mirror_lfs_url)
            yield response_headers
    else:
        if repo_type == "model":
            url = f"{app.app_settings.hf_url}/{org}/{repo}/resolve/{commit}/{file_path}"
        else:
            url = f"{app.app_settings.hf_url}/{repo_type}s/{org}/{repo}/resolve/{commit}/{file_path}"
        async with httpx.AsyncClient() as client:
            async with client.stream(
                method="HEAD", url=url,
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
            ) as response:
                response_headers = response.headers
                response_headers = {k: v for k, v in response_headers.items()}
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(response_headers, ensure_ascii=False))
                if "location" in response_headers:
                    response_headers["location"] = response_headers["location"].replace(app.app_settings.hf_lfs_url, app.app_settings.mirror_lfs_url)
                yield response_headers
                
                async for raw_chunk in response.aiter_raw():
                    if not raw_chunk:
                        continue 
                    yield raw_chunk


async def file_get_generator(app, repo_type: Literal["model", "dataset"], org: str, repo: str, commit: str, file_path: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")
    # save
    repos_path = app.app_settings.repos_path
    save_path = os.path.join(repos_path, f"files/{repo_type}s/{org}/{repo}/resolve/{commit}/{file_path}")
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    
    use_cache = os.path.exists(save_path)

    # proxy
    if use_cache:
        yield request.headers
        with open(save_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk
    else:
        try:
            temp_file_path = None
            if repo_type == "model":
                url = f"{app.app_settings.hf_url}/{org}/{repo}/resolve/{commit}/{file_path}"
            else:
                url = f"{app.app_settings.hf_url}/{repo_type}s/{org}/{repo}/resolve/{commit}/{file_path}"
            async with httpx.AsyncClient() as client:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                    async with client.stream(
                        method="GET", url=url,
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
                    temp_file_path = temp_file.name

                shutil.copyfile(temp_file_path, save_path)
        finally:
            if temp_file_path is not None:
                os.remove(temp_file_path)
