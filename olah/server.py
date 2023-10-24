import datetime
import json
import os
import argparse
import tempfile
import shutil
from typing import Annotated, Union
from fastapi import FastAPI, Header, Request
from fastapi.responses import StreamingResponse
import httpx
from pydantic import BaseSettings
import pytz

app = FastAPI()

class AppSettings(BaseSettings):
    # The address of the model controller.
    repos_path: str = "./repos"

HUGGINGFACE_API_URL = "https://huggingface.co"
HUGGINGFACE_LFS_URL = "https://cdn-lfs.huggingface.co"
MIRROR_API_URL = "http://localhost:8090"
MIRROR_LFS_URL = "http://localhost:8090"
WORKER_API_TIMEOUT = 15
CHUNK_SIZE = 4096
LFS_FILE_BLOCK = 64 * 1024 * 1024

async def meta_generator(app, org: str, model: str, commit: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(repos_path, f"api/models/{org}/{model}/revision/{commit}")
    save_path = os.path.join(save_dir, "meta.json")
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
            async with httpx.AsyncClient() as client:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                    async with client.stream(
                        method="GET", url=f"{HUGGINGFACE_API_URL}/api/models/{org}/{model}/revision/{commit}",
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

async def file_head_generator(app, org: str, model: str, commit: str, file_path: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_path = os.path.join(repos_path, f"{org}/{model}/resolve_head/{commit}/{file_path}")
    save_dir = os.path.dirname(save_path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    
    use_cache = os.path.exists(save_path)

    # proxy
    if use_cache:
        with open(save_path, "r", encoding="utf-8") as f:
            response_headers = json.loads(f.read())
            if "location" in response_headers:
                response_headers["location"] = response_headers["location"].replace(HUGGINGFACE_LFS_URL, MIRROR_LFS_URL)
            yield response_headers
    else:
        async with httpx.AsyncClient() as client:
            async with client.stream(
                method="HEAD", url=f"{HUGGINGFACE_API_URL}/{org}/{model}/resolve/{commit}/{file_path}",
                headers=headers,
                timeout=WORKER_API_TIMEOUT,
            ) as response:
                response_headers = response.headers
                response_headers = {k: v for k, v in response_headers.items()}
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(response_headers, ensure_ascii=False))
                if "location" in response_headers:
                    response_headers["location"] = response_headers["location"].replace(HUGGINGFACE_LFS_URL, MIRROR_LFS_URL)
                yield response_headers
                
                async for raw_chunk in response.aiter_raw():
                    if not raw_chunk:
                        continue 
                    yield raw_chunk


async def file_get_generator(app, org: str, model: str, commit: str, file_path: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")
    # save
    repos_path = app.app_settings.repos_path
    save_path = os.path.join(repos_path, f"{org}/{model}/resolve/{commit}/{file_path}")
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
            async with httpx.AsyncClient() as client:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                    async with client.stream(
                        method="GET", url=f"{HUGGINGFACE_API_URL}/{org}/{model}/resolve/{commit}/{file_path}",
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

async def lfs_get_generator(app, dir1: str, dir2: str, hash1: str, hash2: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(repos_path, f"lfs/{dir1}/{dir2}/{hash1}/{hash2}")
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
    
    # lfs meta
    lfs_meta_path = os.path.join(save_dir, "meta.json")
    if os.path.exists(lfs_meta_path):
        with open(lfs_meta_path, "r", encoding="utf-8") as f:
            lfs_meta = json.loads(f.read())
    else:
        async with httpx.AsyncClient() as client:
            async with client.stream(
                method="GET", url=f"{HUGGINGFACE_LFS_URL}/repos/{dir1}/{dir2}/{hash1}/{hash2}",
                headers={"range": "-"},
                params=request.query_params,
                timeout=WORKER_API_TIMEOUT,
            ) as response:
                file_size = response.headers["content-length"]
                req_headers = {k: v for k, v in response.headers.items()}
        lfs_meta = {
            "lfs_file_block": LFS_FILE_BLOCK,
            "file_size": int(file_size),
            "req_headers": req_headers,
        }
        with open(lfs_meta_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(lfs_meta))
    # range
    file_size = lfs_meta["file_size"]
    if "range" in headers:
        file_range = headers['range'] # 'bytes=1887436800-'
        if file_range.startswith("bytes="):
            file_range = file_range[6:]
        start_pos, end_pos = file_range.split("-")
        if len(start_pos) != 0:
            start_pos = int(start_pos)
        else:
            start_pos = 0
        if len(end_pos) != 0:
            end_pos = int(end_pos)
        else:
            end_pos = file_size - 1
    else:
        start_pos = 0
        end_pos = file_size - 1
    
    # block
    lfs_file_block = lfs_meta["lfs_file_block"]
    start_block = start_pos // lfs_file_block
    end_block = end_pos // lfs_file_block
    
    new_headers = {
        'content-type': 'application/octet-stream',
        'content-length': '1334845784',
        'connection': 'keep-alive',
        'date': datetime.datetime.now(pytz.timezone('GMT')).strftime('%a, %d %b %Y %H:%M:%S %Z'), # 'Tue, 24 Oct 2023 02:36:56 GMT',
        'last-modified': 'Sun, 24 Sep 2023 10:21:01 GMT',
        'etag': '"2869fb6f1f8f0a01cbb19bf22fb28609"',
        'x-amz-storage-class': 'INTELLIGENT_TIERING',
        'x-amz-server-side-encryption': 'AES256',
        'x-amz-version-id': 'Eejziir3Z27bQcoy86pB41xWEDwdSn6u',
        'content-disposition': 'attachment; filename*=UTF-8\'\'model-00008-of-00008.safetensors; filename="model-00008-of-00008.safetensors";',
        'accept-ranges': 'bytes', 'server': 'AmazonS3',
        'x-cache': 'Miss from cloudfront', 'via': '1.1 7110543e95ede37ef1cea5dbc0cc94a4.cloudfront.net (CloudFront)',
        'x-amz-cf-pop': 'HKG54-C1', 'x-amz-cf-id': 'exjRkchC8U5HdiqVlqqQM7rk7zIG2lugrWqXi2YB1yr0_soAb3Bz6w==',
        'cache-control': 'public, max-age=604800, immutable, s-maxage=604800',
        'vary': 'Origin'
    }
    new_headers = lfs_meta["req_headers"]
    new_headers["date"] = datetime.datetime.now(pytz.timezone('GMT')).strftime('%a, %d %b %Y %H:%M:%S %Z')
    new_headers["content-length"] = str(end_pos - start_pos)

    yield new_headers
    cur_pos = start_pos
    cur_block = start_block

    while cur_block <= end_block:
        save_path = os.path.join(save_dir, f"block-{cur_block}.bin")
        use_cache = os.path.exists(save_path)
        chunk_start_pos = cur_block * lfs_file_block
        chunk_end_pos = min((cur_block + 1) * lfs_file_block, file_size)

        # proxy
        if use_cache:
            with open(save_path, "rb") as f:
                if cur_block == start_block:
                    f.seek(start_pos%lfs_file_block)
                    
                while True:
                    raw_chunk = f.read(CHUNK_SIZE)
                    if not raw_chunk:
                        break
                    
                    chunk = raw_chunk
                    if cur_pos > chunk_start_pos:
                        chunk = chunk[cur_pos - chunk_start_pos:]
                    
                    if len(chunk) != 0:
                        yield chunk
                    cur_pos += len(chunk)
        else:
            try:
                temp_file_path = None
                async with httpx.AsyncClient() as client:
                    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                        headers["range"] = f"{chunk_start_pos}-{chunk_end_pos - 1}"
                        async with client.stream(
                            method="GET", url=f"{HUGGINGFACE_LFS_URL}/repos/{dir1}/{dir2}/{hash1}/{hash2}",
                            headers=headers,
                            params=request.query_params,
                            timeout=WORKER_API_TIMEOUT,
                        ) as response:
                            raw_bytes = 0
                            async for raw_chunk in response.aiter_raw():
                                if not raw_chunk:
                                    continue
                                temp_file.write(raw_chunk)

                                stream_chunk = raw_chunk
                                if cur_pos + len(raw_chunk) > chunk_end_pos:
                                    stream_chunk = stream_chunk[:-(cur_pos + len(raw_chunk) - chunk_end_pos)]
                                if cur_pos > chunk_start_pos:
                                    stream_chunk = stream_chunk[chunk_start_pos - cur_pos:]
                                
                                if len(stream_chunk) != 0:
                                    yield stream_chunk
                                cur_pos += len(stream_chunk)
                                raw_bytes += len(raw_chunk)
                                if raw_bytes >= lfs_file_block:
                                    break
                        temp_file_path = temp_file.name
                    shutil.copyfile(temp_file_path, save_path)
            finally:
                if temp_file_path is not None:
                    os.remove(temp_file_path)
        cur_block += 1

    
@app.get("/api/models/{org}/{model}/revision/{commit}")
async def meta_proxy(org: str, model: str, commit: str, request: Request):
    generator = meta_generator(app, org, model, commit, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.head("/{org}/{model}/resolve/{commit}/{file_path:path}")
async def file_head_proxy(org: str, model: str, commit: str, file_path: str, request: Request):
    generator = file_head_generator(app, org, model, commit, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/{org}/{model}/resolve/{commit}/{file_path:path}")
async def file_proxy(org: str, model: str, commit: str, file_path: str, request: Request):
    generator = file_get_generator(app, org, model, commit, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/repos/{dir1}/{dir2}/{hash1}/{hash2}")
async def lfs_proxy(dir1: str, dir2: str, hash1: str, hash2: str, request: Request):
    generator = lfs_get_generator(app, dir1, dir2, hash1, hash2, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--repos-path", type=str, default="./repos")
    args = parser.parse_args()

    app.app_settings = AppSettings(repos_path=args.repos_path)

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)
