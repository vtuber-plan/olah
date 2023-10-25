


import os
import shutil
import tempfile
from typing import Literal
from fastapi import Request

import httpx
from olah.configs import OlahConfig
from olah.constants import CHUNK_SIZE, WORKER_API_TIMEOUT

from olah.utls import check_cache_rules_hf

async def meta_generator(app, repo_type: Literal["model", "dataset"], org: str, repo: str, commit: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(repos_path, f"api/{repo_type}s/{org}/{repo}/revision/{commit}")
    save_path = os.path.join(save_dir, "meta.json")
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    
    use_cache = os.path.exists(save_path)
    allow_cache = await check_cache_rules_hf(app, repo_type, org, repo)
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
            async with httpx.AsyncClient() as client:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                    if not allow_cache:
                        temp_file = open(os.devnull, 'wb')
                    async with client.stream(
                        method="GET", url=f"{app.app_settings.hf_url}/api/{repo_type}s/{org}/{repo}/revision/{commit}",
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
