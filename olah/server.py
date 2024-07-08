import datetime
import json
import os
import argparse
import tempfile
import shutil
from typing import Annotated, Optional, Union
from fastapi import FastAPI, Header, Request
from fastapi.responses import HTMLResponse, StreamingResponse, Response
import httpx
from pydantic import BaseSettings
from olah.configs import OlahConfig
from olah.files import cdn_file_get_generator, file_get_generator, file_head_generator
from olah.lfs import lfs_get_generator
from olah.meta import meta_generator
from olah.utls import check_proxy_rules_hf, check_commit_hf, get_commit_hf, get_newest_commit_hf

app = FastAPI(debug=False)

class AppSettings(BaseSettings):
    # The address of the model controller.
    config: OlahConfig = OlahConfig()
    repos_path: str = "./repos"
    hf_url: str = "https://huggingface.co"
    hf_lfs_url: str = "https://cdn-lfs.huggingface.co"
    mirror_url: str = "http://localhost:8090"
    mirror_lfs_url: str = "http://localhost:8090"

@app.get("/api/{repo_type}/{org_repo}")
async def meta_proxy(repo_type: str, org_repo: str, request: Request):
    if "/" in org_repo and org_repo.count("/") != 1:
        return Response(content="This repository is not accessible.", status_code=404)
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo
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
    generator = meta_generator(app, repo_type, org, repo, commit, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/api/{repo_type}/{org_repo}/revision/{commit}")
async def meta_proxy_commit(repo_type: str, org_repo: str, commit: str, request: Request):
    if "/" in org_repo and org_repo.count("/") != 1:
        return Response(content="This repository is not accessible.", status_code=404)
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    generator = meta_generator(app, repo_type, org, repo, commit, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.head("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
@app.head("/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_head_proxy2(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str = "models"):
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_head_generator(app, repo_type, org, repo, commit_sha, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.head("/{repo_type}/{org_repo}/resolve/{commit}/{file_path:path}")
@app.head("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_head_proxy(org_repo: str, commit: str, file_path: str, request: Request, repo_type: str = "models"):
    if "/" in org_repo and org_repo.count("/") != 1:
        return Response(content="This repository is not accessible.", status_code=404)
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_head_generator(app, repo_type, org, repo, commit_sha, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/{repo_type}/{org}/{repo}/resolve/{commit}/{file_path:path}")
@app.get("/{org}/{repo}/resolve/{commit}/{file_path:path}")
async def file_proxy2(org: str, repo: str, commit: str, file_path: str, request: Request, repo_type: str = "models"):
    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/{repo_type}/{org_repo}/resolve/{commit}/{file_path:path}")
@app.get("/{org_repo}/resolve/{commit}/{file_path:path}")
async def file_proxy(org_repo: str, commit: str, file_path: str, request: Request, repo_type: str = "models"):
    if "/" in org_repo and org_repo.count("/") != 1:
        return Response(content="This repository is not accessible.", status_code=404)
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)
    if not await check_commit_hf(app, repo_type, org, repo, commit):
        return Response(content="This repository is not accessible. ", status_code=404)
    commit_sha = await get_commit_hf(app, repo_type, org, repo, commit)
    generator = await file_get_generator(app, repo_type, org, repo, commit_sha, file_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/{repo_type}/{org_repo}/{hash_file}")
async def cdn_file_proxy(org_repo: str, hash_file: str, request: Request, repo_type: str = "models"):
    if "/" in org_repo and org_repo.count("/") != 1:
        return Response(content="This repository is not accessible.", status_code=404)
    if "/" in org_repo:
        org, repo = org_repo.split("/")
    else:
        org = None
        repo = org_repo

    if not await check_proxy_rules_hf(app, repo_type, org, repo):
        return Response(content="This repository is forbidden by the mirror. ", status_code=403)

    generator = await cdn_file_get_generator(app, repo_type, org, repo, hash_file, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)


@app.get("/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}")
async def lfs_proxy(dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request):
    repo_type = "models"
    lfs_url = f"{app.app_settings.hf_lfs_url}/repos/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    save_path = f"{dir1}/{dir2}/{hash_repo}/{hash_file}"
    generator = lfs_get_generator(app, repo_type, lfs_url, save_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

@app.get("/datasets/hendrycks_test/{hash_file}")
async def lfs_proxy(hash_file: str, request: Request):
    repo_type = "datasets"
    lfs_url = f"{app.app_settings.hf_lfs_url}/datasets/hendrycks_test/{hash_file}"
    save_path = f"hendrycks_test/{hash_file}"
    generator = lfs_get_generator(app, repo_type, lfs_url, save_path, request)
    headers = await generator.__anext__()
    return StreamingResponse(generator, headers=headers)

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
    parser.add_argument("--ssl-key", type=str, default=None)
    parser.add_argument("--ssl-cert", type=str, default=None)
    parser.add_argument("--repos-path", type=str, default="./repos")
    parser.add_argument("--hf-url", type=str, default="https://huggingface.co")
    parser.add_argument("--hf-lfs-url", type=str, default="https://cdn-lfs.huggingface.co")
    parser.add_argument("--mirror-url", type=str, default="http://localhost:8090")
    parser.add_argument("--mirror-lfs-url", type=str, default="http://localhost:8090")
    args = parser.parse_args()
    
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
    if is_default_value(args, "hf_url"):
        args.hf_url = config.hf_url
    if is_default_value(args, "hf_lfs_url"):
        args.hf_lfs_url = config.hf_lfs_url
    if is_default_value(args, "mirror_url"):
        args.mirror_url = config.mirror_url
    if is_default_value(args, "mirror_lfs_url"):
        args.mirror_lfs_url = config.mirror_lfs_url

    app.app_settings = AppSettings(
        config=config,
        repos_path=args.repos_path,
        hf_url=args.hf_url,
        hf_lfs_url=args.hf_lfs_url,
        mirror_url=args.mirror_url,
        mirror_lfs_url=args.mirror_lfs_url,
    )

    import uvicorn
    if __name__ == "__main__":
        uvicorn.run(
            "olah.server:app",
            host=args.host,
            port=args.port,
            log_level="info",
            reload=True,
            ssl_keyfile=args.ssl_key,
            ssl_certfile=args.ssl_cert
        )
