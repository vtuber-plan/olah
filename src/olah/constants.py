# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os


WORKER_API_TIMEOUT = 15
# Kept under huggingface_hub's 10s read timeout: timing out on headers fails its
# download, whereas a stalled body is resumed.
HEADER_HOLD_TIMEOUT = 5
CHUNK_SIZE = 4096
LFS_FILE_BLOCK = 64 * 1024 * 1024

DEFAULT_LOGGER_DIR = "./logs"
OLAH_CODE_DIR = os.path.dirname(os.path.abspath(__file__))

ORIGINAL_LOC = "oriloc"

from huggingface_hub.constants import (
    REPO_TYPES_MAPPING,
    HUGGINGFACE_CO_URL_TEMPLATE,
    HUGGINGFACE_HEADER_X_REPO_COMMIT,
    HUGGINGFACE_HEADER_X_LINKED_ETAG,
    HUGGINGFACE_HEADER_X_LINKED_SIZE,
)
