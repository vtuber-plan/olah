# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import argparse
import datetime
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Sequence, Tuple, Union

import httpx
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi_utils.tasks import repeat_every

from olah.configs import OlahConfig
from olah.constants import OLAH_CODE_DIR
from olah.errors import error_page_not_found
from olah.server_routes import (
    cdn_proxy_common,
    commits_proxy_common,
    file_get_common,
    file_head_common,
    lfs_proxy_common,
    meta_proxy_common,
    pathsinfo_proxy_common,
    router,
    tree_proxy_common,
)
from olah.utils.disk_utils import (
    convert_bytes_to_human_readable,
    convert_to_bytes,
    get_folder_size,
    sort_files_by_access_time,
    sort_files_by_modify_time,
    sort_files_by_size,
)
from olah.utils.logging import build_logger


BASE_SETTINGS = False
if not BASE_SETTINGS:
    try:
        from pydantic import BaseSettings
        BASE_SETTINGS = True
    except ImportError:
        BASE_SETTINGS = False

if not BASE_SETTINGS:
    try:
        from pydantic_settings import BaseSettings
        BASE_SETTINGS = True
    except ImportError:
        BASE_SETTINGS = False

if not BASE_SETTINGS:
    raise Exception("Cannot import BaseSettings from pydantic or pydantic-settings")


logger = None


async def check_connection(url: str) -> bool:
    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method="HEAD",
                url=url,
                timeout=10,
            )
        return response.status_code == 200
    except httpx.TimeoutException:
        return False


@repeat_every(seconds=60 * 5)
async def check_hf_connection() -> None:
    if app.state.app_settings.config.offline:
        return
    scheme = app.state.app_settings.config.hf_scheme
    netloc = app.state.app_settings.config.hf_netloc
    hf_online_status = await check_connection(
        f"{scheme}://{netloc}/datasets/Salesforce/wikitext/resolve/main/.gitattributes"
    )
    if not hf_online_status:
        print("Failed to reach Huggingface Site.", file=sys.stderr)


@repeat_every(seconds=60 * 60)
async def check_disk_usage() -> None:
    if app.state.app_settings.config.offline:
        return
    if app.state.app_settings.config.cache_size_limit is None:
        return

    limit_size = app.state.app_settings.config.cache_size_limit
    current_size = get_folder_size(app.state.app_settings.config.repos_path)

    limit_size_h = convert_bytes_to_human_readable(limit_size)
    current_size_h = convert_bytes_to_human_readable(current_size)

    if current_size < limit_size:
        return
    print(f"Cache size exceeded! Limit: {limit_size_h}, Current: {current_size_h}.")
    print("Cleaning...")
    files_path = os.path.join(app.state.app_settings.config.repos_path, "files")
    lfs_path = os.path.join(app.state.app_settings.config.repos_path, "lfs")

    files: Sequence[Tuple[str, Union[int, datetime.datetime]]] = []
    if app.state.app_settings.config.cache_clean_strategy == "LRU":
        files = sort_files_by_access_time(files_path) + sort_files_by_access_time(lfs_path)
        files = sorted(files, key=lambda x: x[1])
    elif app.state.app_settings.config.cache_clean_strategy == "FIFO":
        files = sort_files_by_modify_time(files_path) + sort_files_by_modify_time(lfs_path)
        files = sorted(files, key=lambda x: x[1])
    elif app.state.app_settings.config.cache_clean_strategy == "LARGE_FIRST":
        files = sort_files_by_size(files_path) + sort_files_by_size(lfs_path)
        files = sorted(files, key=lambda x: x[1], reverse=True)

    for filepath, _ in files:
        if current_size < limit_size:
            break
        filesize = os.path.getsize(filepath)
        os.remove(filepath)
        current_size -= filesize
        print(f"Remove file: {filepath}. File Size: {convert_bytes_to_human_readable(filesize)}")

    current_size = get_folder_size(app.state.app_settings.config.repos_path)
    current_size_h = convert_bytes_to_human_readable(current_size)
    print(f"Cleaning finished. Limit: {limit_size_h}, Current: {current_size_h}.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await check_hf_connection()
    await check_disk_usage()
    yield


app = FastAPI(lifespan=lifespan, debug=False)
templates = Jinja2Templates(directory=os.path.join(OLAH_CODE_DIR, "static"))
app.state.templates = templates
app.state.logger = None
app.include_router(router)


class AppSettings(BaseSettings):
    config: OlahConfig = OlahConfig()


@app.exception_handler(404)
async def custom_404_handler(_, __):
    return error_page_not_found()


def init():
    global logger
    parser = argparse.ArgumentParser(description="Olah Huggingface Mirror Server.")
    parser.add_argument("--config", "-c", type=str, default="")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--hf-scheme", type=str, default="https", help="The scheme of huggingface site (http or https)")
    parser.add_argument("--hf-netloc", type=str, default="huggingface.co")
    parser.add_argument("--hf-lfs-netloc", type=str, default="cdn-lfs.huggingface.co")
    parser.add_argument("--mirror-scheme", type=str, default="http", help="The scheme of mirror site (http or https)")
    parser.add_argument("--mirror-netloc", type=str, default="localhost:8090")
    parser.add_argument("--mirror-lfs-netloc", type=str, default="localhost:8090")
    parser.add_argument("--has-lfs-site", action="store_true")
    parser.add_argument("--ssl-key", type=str, default=None, help="The SSL key file path, if HTTPS is used")
    parser.add_argument("--ssl-cert", type=str, default=None, help="The SSL cert file path, if HTTPS is used")
    parser.add_argument("--repos-path", type=str, default="./repos", help="The folder to save cached repositories")
    parser.add_argument("--cache-size-limit", type=str, default="", help="The limit size of cache. (Example values: '100MB', '2GB', '500KB')")
    parser.add_argument("--cache-clean-strategy", type=str, default="LRU", help="The clean strategy of cache. ('LRU', 'FIFO', 'LARGE_FIRST')")
    parser.add_argument("--log-path", type=str, default="./logs", help="The folder to save logs")
    args = parser.parse_args()

    logger = build_logger("olah", "olah.log", logger_dir=args.log_path)
    app.state.logger = logger

    def is_default_value(namespace, arg_name):
        if hasattr(namespace, arg_name):
            arg_value = getattr(namespace, arg_name)
            arg_default = parser.get_default(arg_name)
            return arg_value == arg_default
        return False

    if args.config != "":
        config = OlahConfig(args.config)
    else:
        config = OlahConfig()

        if not is_default_value(args, "host"):
            config.host = args.host
        if not is_default_value(args, "port"):
            config.port = args.port

        if not is_default_value(args, "ssl_key"):
            config.ssl_key = args.ssl_key
        if not is_default_value(args, "ssl_cert"):
            config.ssl_cert = args.ssl_cert

        if not is_default_value(args, "repos_path"):
            config.repos_path = args.repos_path
        if not is_default_value(args, "hf_scheme"):
            config.hf_scheme = args.hf_scheme
        if not is_default_value(args, "hf_netloc"):
            config.hf_netloc = args.hf_netloc
        if not is_default_value(args, "hf_lfs_netloc"):
            config.hf_lfs_netloc = args.hf_lfs_netloc
        if not is_default_value(args, "mirror_scheme"):
            config.mirror_scheme = args.mirror_scheme
        if not is_default_value(args, "mirror_netloc"):
            config.mirror_netloc = args.mirror_netloc
        if not is_default_value(args, "mirror_lfs_netloc"):
            config.mirror_lfs_netloc = args.mirror_lfs_netloc
        if not is_default_value(args, "cache_size_limit"):
            config.cache_size_limit = convert_to_bytes(args.cache_size_limit)
        if not is_default_value(args, "cache_clean_strategy"):
            config.cache_clean_strategy = args.cache_clean_strategy
        elif not args.has_lfs_site and not is_default_value(args, "mirror_netloc"):
            config.mirror_lfs_netloc = args.mirror_netloc

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

    if is_default_value(args, "hf_scheme"):
        args.hf_scheme = config.hf_scheme
    if is_default_value(args, "hf_netloc"):
        args.hf_netloc = config.hf_netloc
    if is_default_value(args, "hf_lfs_netloc"):
        args.hf_lfs_netloc = config.hf_lfs_netloc
    if is_default_value(args, "mirror_scheme"):
        args.mirror_scheme = config.mirror_scheme
    if is_default_value(args, "mirror_netloc"):
        args.mirror_netloc = config.mirror_netloc
    if is_default_value(args, "mirror_lfs_netloc"):
        args.mirror_lfs_netloc = config.mirror_lfs_netloc

    if is_default_value(args, "cache_size_limit"):
        args.cache_size_limit = config.cache_size_limit
    if is_default_value(args, "cache_clean_strategy"):
        args.cache_clean_strategy = config.cache_clean_strategy

    if "," in args.host:
        args.host = args.host.split(",")

    args.mirror_scheme = config.mirror_scheme = "http" if args.ssl_key is None else "https"

    print(args)
    if config.cache_size_limit is not None:
        logger.info(
            f"""
======== WARNING ========
Due to the cache_size_limit parameter being set, Olah will periodically delete cache files.
Please ensure that the cache directory specified in repos_path '{config.repos_path}' is correct.
Incorrect settings may result in unintended file deletion and loss!!! !!!
========================="""
        )
        for _ in range(10):
            time.sleep(0.2)

    app.state.app_settings = AppSettings(config=config)
    return args


def run_server(args):
    import uvicorn

    uvicorn.run(
        "olah.server:app",
        host=args.host,
        port=args.port,
        log_level="info",
        reload=False,
        ssl_keyfile=args.ssl_key,
        ssl_certfile=args.ssl_cert,
    )


def _run_cli():
    args = init()
    run_server(args)


def main():
    _run_cli()


def cli():
    _run_cli()


if __name__ == "__main__":
    main()
