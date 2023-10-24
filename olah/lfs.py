import datetime
import json
import os
import tempfile
import shutil
from fastapi import FastAPI, Header, Request
import httpx
import pytz

from olah.constants import CHUNK_SIZE, LFS_FILE_BLOCK, WORKER_API_TIMEOUT


async def lfs_get_generator(app, repo_type: str, lfs_url: str, save_path: str, request: Request):
    headers = {k: v for k, v in request.headers.items()}
    headers.pop("host")

    # save
    repos_path = app.app_settings.repos_path
    save_dir = os.path.join(repos_path, f"lfs/{repo_type}s/{save_path}")
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
                method="GET", url=lfs_url,
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
            end_pos = file_size
    else:
        start_pos = 0
        end_pos = file_size

    # block
    lfs_file_block = lfs_meta["lfs_file_block"]
    start_block = start_pos // lfs_file_block
    end_block = end_pos // lfs_file_block

    new_headers = lfs_meta["req_headers"]
    new_headers["date"] = datetime.datetime.now(pytz.timezone('GMT')).strftime('%a, %d %b %Y %H:%M:%S %Z')
    new_headers["content-length"] = str(end_pos - start_pos)

    yield new_headers
    cur_pos = start_pos
    cur_block = start_block

    while cur_block <= end_block:
        save_path = os.path.join(save_dir, f"block-{cur_block}.bin")
        use_cache = os.path.exists(save_path)
        block_start_pos = cur_block * lfs_file_block
        block_end_pos = min((cur_block + 1) * lfs_file_block, file_size)

        # proxy
        if use_cache:
            with open(save_path, "rb") as f:
                sub_chunk_start_pos = block_start_pos
                while True:
                    raw_chunk = f.read(CHUNK_SIZE)
                    if not raw_chunk:
                        break

                    chunk = raw_chunk
                    if cur_pos > sub_chunk_start_pos:
                        chunk = chunk[cur_pos - sub_chunk_start_pos:]
                    
                    if len(chunk) != 0:
                        yield chunk
                    cur_pos += len(chunk)
                    sub_chunk_start_pos += len(raw_chunk)
        else:
            try:
                temp_file_path = None
                async with httpx.AsyncClient() as client:
                    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                        headers["range"] = f"bytes={block_start_pos}-{block_end_pos - 1}"
                        async with client.stream(
                            method="GET", url=lfs_url,
                            headers=headers,
                            params=request.query_params,
                            timeout=WORKER_API_TIMEOUT,
                        ) as response:
                            raw_bytes = 0
                            sub_chunk_start_pos = block_start_pos
                            async for raw_chunk in response.aiter_raw():
                                if not raw_chunk:
                                    continue
                                temp_file.write(raw_chunk)

                                stream_chunk = raw_chunk
                                # if cur_pos + len(raw_chunk) > chunk_end_pos:
                                #     stream_chunk = stream_chunk[:-(cur_pos + len(raw_chunk) - chunk_end_pos)]
                                if cur_pos > sub_chunk_start_pos and cur_pos < sub_chunk_start_pos + len(raw_chunk):
                                    stream_chunk = stream_chunk[cur_pos - sub_chunk_start_pos:]

                                if len(stream_chunk) != 0:
                                    yield stream_chunk
                                cur_pos += len(stream_chunk)
                                raw_bytes += len(raw_chunk)
                                sub_chunk_start_pos += len(raw_chunk)
                                if raw_bytes >= block_end_pos - block_start_pos:
                                    break
                        temp_file_path = temp_file.name
                    shutil.copyfile(temp_file_path, save_path)
            finally:
                if temp_file_path is not None:
                    os.remove(temp_file_path)
        cur_block += 1
