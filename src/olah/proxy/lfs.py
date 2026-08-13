# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
from typing import Literal
from fastapi import FastAPI, Header, Request

from olah.proxy.files import _file_realtime_stream
from olah.utils.file_utils import make_dirs


async def lfs_head_generator(
    app: FastAPI,
    dir1: str,
    dir2: str,
    hash_repo: str,
    hash_file: str,
    request: Request,
    allow_cache: bool,
):
    # save
    repos_path = app.state.app_settings.config.repos_path
    head_path = os.path.join(
        repos_path, f"lfs/heads/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    save_path = os.path.join(
        repos_path, f"lfs/files/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # LFS objects are content-addressed: hash_file IS the SHA-256 of the blob, so
    # it is used both as the cache's identity (expected_etag) and the response
    # etag. Authorization + cache permission are decided by lfs_proxy_common
    # before dispatching here.
    return await _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=str(request.url),
        request=request,
        method="HEAD",
        allow_cache=allow_cache,
        commit=None,
        expected_etag=hash_file,
    )


async def lfs_get_generator(
    app: FastAPI,
    dir1: str,
    dir2: str,
    hash_repo: str,
    hash_file: str,
    request: Request,
    allow_cache: bool,
):
    # save
    repos_path = app.state.app_settings.config.repos_path
    head_path = os.path.join(
        repos_path, f"lfs/heads/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    save_path = os.path.join(
        repos_path, f"lfs/files/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    return await _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=str(request.url),
        request=request,
        method="GET",
        allow_cache=allow_cache,
        commit=None,
        expected_etag=hash_file,
    )
