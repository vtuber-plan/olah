# coding=utf-8
# Copyright 2024 XiaHan
# 
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
import argparse
from typing import Annotated, Optional, Union
from urllib.parse import urljoin
from fastapi import FastAPI, Header, Request
from fastapi.responses import HTMLResponse, StreamingResponse, Response
import httpx
from pydantic import BaseSettings
from olah.configs import OlahConfig
from olah.files import cdn_file_get_generator, file_get_generator
from olah.lfs import lfs_get_generator, lfs_head_generator
from olah.meta import meta_generator, meta_proxy_cache
from olah.utils.url_utils import check_proxy_rules_hf, check_commit_hf, get_commit_hf, get_newest_commit_hf, parse_org_repo

from olah.utils.logging import build_logger

app = FastAPI(debug=False)

class AppSettings(BaseSettings):
    # The address of the model controller.
    config: OlahConfig = OlahConfig()
    repos_path: str = "./repos"

# ======================
# API Hooks
# ======================
@app.get("/api/{repo_type}/{org_repo}")
async def meta_proxy(repo_type: str, org_repo: str, request: Request):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return Response(content="This repository is not accessible.", status_code=404)

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror.", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, None):
        return Response(content="This repository is not accessible.", status_code=404)
    new_commit = await get_newest_commit_hf(app, repo_type, org, repo)
    generator = meta_generator(app, repo_type, org, repo, new_commit, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/api/{repo_type}/{org}/{repo}/revision/{commit}")
async def meta_proxy_commit2(repo_type: str, org: str, repo: str, commit: str, request: Request):
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)

    # if branch name and online mode, refresh branch info
    if commit_sha != commit and not app.app_settings.config.offline:
        await meta_proxy_cache(app, repo_type, org, repo, commit, request)

    generator = meta_generator(app, repo_type, org, repo, commit_sha, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/api/{repo_type}/{org_repo}/revision/{commit}")
async def meta_proxy_commit(repo_type: str, org_repo: str, commit: str, request: Request):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return Response(content="This repository is not accessible.", status_code=404)

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)

    # if branch name and online mode, refresh branch info
    if commit_sha != commit and not app.app_settings.config.offline:
        await meta_proxy_cache(app, repo_type, org, repo, commit, request)

    generator = meta_generator(app, repo_type, org, repo, commit_sha, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)


# ======================
# File Head Hooks
# ======================
@app.head("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_head3(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str):
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path=file_path, method="HEAD", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.head("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_head2(org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request):
    if org_or_repo_type in ["models", "datasets", "spaces"]:
        repo_type: str = org_or_repo_type
        org, repo = parse_org_repo(repo_name)
        if org is None and repo is None:
            return Response(content="This repository is not accessible.", status_code=404)
    else:
        repo_type: str = "models"
        org, repo = org_or_repo_type, repo_name

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if org is not None and not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path=file_path, method="HEAD", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.head("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_head(org_repo: str, commit: str, file_path: str, request: Request):
    repo_type: str = "models"
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return Response(content="This repository is not accessible.", status_code=404)

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if org is not None and not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path=file_path, method="HEAD", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.head("/{org_repo}/{hash_file}")
@app.head("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_head(org_repo: str, hash_file: str, request: Request, repo_type: str = "models"):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return Response(content="This repository is not accessible.", status_code=404)

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)

    generator = await cdn_file_get_generator(app, repo_type, org, repo, hash_file, method="HEAD", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

# ======================
# File Hooks
# ======================
@app.get("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_get3(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str):
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path=file_path, method="GET", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.get("/{org_or_repo_type}/{repo_name}/resolve/{commit}/{file_path:path}")
async def file_get2(org_or_repo_type: str, repo_name: str, commit: str, file_path: str, request: Request):
    if org_or_repo_type in ["models", "datasets", "spaces"]:
        repo_type: str = org_or_repo_type
        org, repo = parse_org_repo(repo_name)
        if org is None and repo is None:
            return Response(content="This repository is not accessible.", status_code=404)
    else:
        repo_type: str = "models"
        org, repo = org_or_repo_type, repo_name

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if org is not None and not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path=file_path, method="GET", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.get("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_get(org_repo: str, commit: str, file_path: str, request: Request):
    repo_type: str = "models"
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return Response(content="This repository is not accessible.", status_code=404)

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path=file_path, method="GET", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.get("/{org_repo}/{hash_file}")
@app.get("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_get(org_repo: str, hash_file: str, request: Request, repo_type: str = "models"):
    org, repo = parse_org_repo(org_repo)
    if org is None and repo is None:
        return Response(content="This repository is not accessible.", status_code=404)

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)

    generator = await cdn_file_get_generator(app, repo_type, org, repo, hash_file, method="GET", request=request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

# ======================
# LFS Hooks
# ======================
@app.head("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_head(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    generator = await lfs_head_generator(app, dir1, dir2, hash_repo, hash_file, request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

@app.get("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_get(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    generator = await lfs_get_generator(app, dir1, dir2, hash_repo, hash_file, request)
    status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers, status_code=status_code)

# ======================
# Web Page Hooks
# ======================
@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(os.path.dirname(__file__), "../static/index.html"), "r", encoding="utf-8") as f:
        page = f.read()
    return page

if __name__ in ["__main__", "olah.server"]:
    parser = argparse.ArgumentParser(
        description="Olah Huggingface Mirror Server."
    )
    parser.add_argument("--config", "-c", type=str, default="")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--ssl-key", type=str, default=None, help="The SSL key file path, if HTTPS is used")
    parser.add_argument("--ssl-cert", type=str, default=None, help="The SSL cert file path, if HTTPS is used")
    parser.add_argument("--repos-path", type=str, default="./repos", help="The folder to save cached repositories")
    parser.add_argument("--log-path", type=str, default="./logs", help="The folder to save logs")
    args = parser.parse_args()
    print(args)

    logger = build_logger("olah", "olah.log", logger_dir=args.log_path)
    
    def is_default_value(args, arg_name):
        if hasattr(args, arg_name):
            arg_value = getattr(args, arg_name)
            arg_default = parser.get_default(arg_name)
            return arg_value == arg_default
        return False

    if args.config != "":
        config = OlahConfig(args.config)
    else:
        config = OlahConfig()
    
    if is_default_value(args, "host"):
        args.host = config.host
    if is_default_value(args, "port"):
        args.port = config.port
    if is_default_value(args, "ssl_key"):
        args.ssl_key = config.ssl_key
    if is_default_value(args, "ssl_cert"):
        args.ssl_cert = config.ssl_cert
    if is_default_value(args, "repos_path"):
        args.repos_path = config.repos_path

    app.app_settings = AppSettings(
        config=config,
        repos_path=args.repos_path,
    )

    import uvicorn
    if __name__ == "__main__":
        uvicorn.run(
            "olah.server:app",
            host=args.host,
            port=args.port,
            log_level="info",
            reload=False,
            ssl_keyfile=args.ssl_key,
            ssl_certfile=args.ssl_cert
        )
