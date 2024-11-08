# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os


def make_dirs(path: str):
    if os.path.isdir(path):
        save_dir = path
    else:
        save_dir = os.path.dirname(path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
