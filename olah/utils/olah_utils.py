# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import platform
import os


def get_olah_path() -> str:
    if platform.system() == "Windows":
        olah_path = os.path.expanduser("~\\.olah")
    else:
        olah_path = os.path.expanduser("~/.olah")
    return olah_path
