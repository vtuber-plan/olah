# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

WORKER_API_TIMEOUT = 15
CHUNK_SIZE = 4096
LFS_FILE_BLOCK = 64 * 1024 * 1024

DEFAULT_LOGGER_DIR = "./logs"

ORIGINAL_LOC = "oriloc"

from huggingface_hub.constants import (
    REPO_TYPES_MAPPING,
    HUGGINGFACE_CO_URL_TEMPLATE,
    HUGGINGFACE_HEADER_X_REPO_COMMIT,
    HUGGINGFACE_HEADER_X_LINKED_ETAG,
    HUGGINGFACE_HEADER_X_LINKED_SIZE,
)
