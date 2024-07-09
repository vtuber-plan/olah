


import os
import shutil
import tempfile
from typing import Dict, Literal
from fastapi import FastAPI, Request

import httpx
from olah.configs import OlahConfig
from olah.constants import CHUNK_SIZE, WORKER_API_TIMEOUT

from olah.utils import check_cache_rules_hf, get_org_repo, make_dirs

async def meta_cache_generator(app: FastAPI, save_path: str):
    yield {}
    with open(save_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

async def meta_proxy_generator(app: FastAPI, headers: Dict[str, str], meta_url: str, allow_cache: bool, save_path: str):
    try:
        temp_file_path = None
        async with httpx.AsyncClient(follow_redirects=True) as client:
            with tempfile.NamedTemporaryFile(mode="wb", delete=True) as temp_file:
                temp_file_path = temp_file.name
                if not allow_cache:
                    write_temp_file = False
                else:
                    write_temp_file = True
                async with client.stream(
                    method="GET", url=meta_url,
                    headers=headers,
                    timeout=WORKER_API_TIMEOUT,
                ) as response:
                    response_headers = response.headers
                    yield response_headers

                    async for raw_chunk in response.aiter_raw():
                        if not raw_chunk:
                            continue
                        if write_temp_file:
                            temp_file.write(raw_chunk)
                        yield raw_chunk
                if temp_file_path is not None:
                    temp_file.flush()
                    shutil.copyfile(temp_file_path, save_path)
    finally:
        if temp_file_path is not None and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

async def meta_generator(app: FastAPI, repo_type: Literal["models", "datasets"], org: str, repo: str, commit: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(repos_path, f"api/{repo_type}/{org}/{repo}/revision/{commit}")
    save_path = os.path.join(save_dir, "meta.json")
    make_dirs(save_path)

    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)

    org_repo = get_org_repo(org, repo)
    meta_url = f"{app.app_settings.hf_url}/api/{repo_type}/{org_repo}/revision/{commit}"
    # proxy
    if use_cache:
        async for item in meta_cache_generator(app, save_path):
            yield item
    else:
        async for item in meta_proxy_generator(app, headers, meta_url, allow_cache, save_path):
            yield item
