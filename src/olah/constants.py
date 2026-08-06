# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os

# Legacy scalar kept for imports that pass timeout=WORKER_API_TIMEOUT to httpx.
# Prefer olah.utils.http_utils.worker_api_timeout() / worker_stream_timeout().
WORKER_API_TIMEOUT = 15
# Larger read buffer improves throughput for upstream streaming.
CHUNK_SIZE = 256 * 1024
# Align with huggingface_hub LFS chunk size and common Range clients (64 MiB).
LFS_FILE_BLOCK = 64 * 1024 * 1024

# Cache tuning (also configurable via configs.toml [performance]).
OLAH_CACHE_BLOCK_SIZE = int(os.getenv("OLAH_CACHE_BLOCK_SIZE", str(LFS_FILE_BLOCK)))
OLAH_CACHE_GZIP_LEVEL = int(os.getenv("OLAH_CACHE_GZIP_LEVEL", "1"))
OLAH_REMOTE_RETRY_MAX = int(os.getenv("OLAH_REMOTE_RETRY_MAX", "5"))
# Remote ranges larger than this are split at block boundaries before fetch (64 MiB default).
OLAH_REMOTE_FETCH_BLOCK_ALIGN = os.getenv("OLAH_REMOTE_FETCH_BLOCK_ALIGN", "1").lower() in (
    "1",
    "true",
    "yes",
)
OLAH_CACHE_SHA256_VERIFY = os.getenv("OLAH_CACHE_SHA256_VERIFY", "1").lower() in (
    "1",
    "true",
    "yes",
)

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
