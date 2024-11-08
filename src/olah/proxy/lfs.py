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
    app, dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request
):
    # save
    repos_path = app.app_settings.config.repos_path
    head_path = os.path.join(
        repos_path, f"lfs/heads/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    save_path = os.path.join(
        repos_path, f"lfs/files/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # use_cache = os.path.exists(head_path) and os.path.exists(save_path)
    allow_cache = True

    # proxy
    return _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=str(request.url),
        request=request,
        method="HEAD",
        allow_cache=allow_cache,
        commit=None,
    )


async def lfs_get_generator(
    app, dir1: str, dir2: str, hash_repo: str, hash_file: str, request: Request
):
    # save
    repos_path = app.app_settings.config.repos_path
    head_path = os.path.join(
        repos_path, f"lfs/heads/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    save_path = os.path.join(
        repos_path, f"lfs/files/{dir1}/{dir2}/{hash_repo}/{hash_file}"
    )
    make_dirs(head_path)
    make_dirs(save_path)

    # use_cache = os.path.exists(head_path) and os.path.exists(save_path)
    allow_cache = True

    # proxy
    return _file_realtime_stream(
        app=app,
        save_path=save_path,
        head_path=head_path,
        url=str(request.url),
        request=request,
        method="GET",
        allow_cache=allow_cache,
        commit=None,
    )
