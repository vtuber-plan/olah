import datetime
import json
import os
import argparse
import tempfile
import shutil
from typing import Annotated, Union
from fastapi import FastAPI, Header, Request
from fastapi.responses import StreamingResponse, Response
import httpx
from pydantic import BaseSettings
import pytz
from olah.files import file_get_generator, file_head_generator
from olah.lfs import lfs_get_generator

from olah.meta import check_commit_hf, meta_generator

app = FastAPI(debug=False)

class AppSettings(BaseSettings):
    # The address of the model controller.
    repos_path: str = "./repos"
    hf_url: str = "https://huggingface.co"
    hf_lfs_url: str = "https://cdn-lfs.huggingface.co"
    mirror_url: str = "http://localhost:8090"
    mirror_lfs_url: str = "http://localhost:8090"


@app.get("/api/{repo_type}s/{org}/{repo}/revision/{commit}")
async def meta_proxy(repo_type: str, org: str, repo: str, commit: str, request: Request):
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(status_code=404)
    generator = meta_generator(app, repo_type, org, repo, commit, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.head("/{repo_type}s/{org}/{repo}/resolve/{commit}/{file_path:path}")
@app.head("/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_head_proxy(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str = "model"):
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(status_code=404)
    generator = file_head_generator(app, repo_type, org, repo, commit, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/{repo_type}s/{org}/{repo}/resolve/{commit}/{file_path:path}")
@app.get("/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_proxy(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str = "model"):
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(status_code=404)
    generator = file_get_generator(app, repo_type, org, repo, commit, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_proxy(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    repo_type = "model"
    lfs_url = f"{app.app_settings.hf_lfs_url}/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    save_path = f"{dir1}/{dir2}/{hash_repo}/{hash_file}"
    generator = lfs_get_generator(app, repo_type, lfs_url, save_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/datasets/hendrycks_test/{hash_file}")
async def lfs_proxy(hash_file: str, request: Request):
    repo_type = "dataset"
    lfs_url = f"{app.app_settings.hf_lfs_url}/datasets/hendrycks_test/{hash_file}"
    save_path = f"hendrycks_test/{hash_file}"
    generator = lfs_get_generator(app, repo_type, lfs_url, save_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

if __name__ in ["__main__", "olah.server"]:
    parser = argparse.ArgumentParser(
        description="Olah Huggingface Mirror Server."
    )
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--repos-path", type=str, default="./repos")
    parser.add_argument("--hf-url", type=str, default="https://huggingface.co")
    parser.add_argument("--hf-lfs-url", type=str, default="https://cdn-lfs.huggingface.co")
    parser.add_argument("--mirror-url", type=str, default="http://localhost:8090")
    parser.add_argument("--mirror-lfs-url", type=str, default="http://localhost:8090")
    args = parser.parse_args()

    app.app_settings = AppSettings(repos_path=args.repos_path)

    import uvicorn
    if __name__ == "__main__":
        uvicorn.run("olah.server:app", host=args.host, port=args.port)
