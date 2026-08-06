# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import hashlib
import logging
import lzma
import mmap
import os
import tempfile
import string
import struct
import threading
import time
import gzip
from typing import BinaryIO, Dict, List, Optional

logger = logging.getLogger(__name__)

import aiofiles
import fastapi
import fastapi.concurrency
import portalocker
from olah.constants import OLAH_CACHE_BLOCK_SIZE, OLAH_CACHE_GZIP_LEVEL
from .bitset import Bitset

CURRENT_OLAH_CACHE_VERSION = 9
# 64 MiB — aligned with huggingface_hub / olares-ollama Range chunk size.
DEFAULT_BLOCK_SIZE = OLAH_CACHE_BLOCK_SIZE
MAX_BLOCK_NUM = 8192
DEFAULT_COMPRESSION_ALGO = 1
GZIP_MAGIC = b"\x1f\x8b"
STALE_TMP_MAX_AGE_SEC = 3600
"""
0: no compression
1: gzip
2: lzma
3: blosc
4: zlib
5: zstd
6: ...
"""

class OlahCacheHeader(object):
    MAGIC_NUMBER = "OLAH".encode("ascii")
    HEADER_FIX_SIZE = 36

    def __init__(
        self,
        version: int = CURRENT_OLAH_CACHE_VERSION,
        block_size: int = DEFAULT_BLOCK_SIZE,
        file_size: int = 0,
        compression_algo: int = DEFAULT_COMPRESSION_ALGO,
    ) -> None:
        self._version = version
        self._block_size = block_size
        self._file_size = file_size
        self._compression_algo = compression_algo

        self._block_number = (file_size + block_size - 1) // block_size

    @property
    def version(self) -> int:
        return self._version

    @property
    def block_size(self) -> int:
        return self._block_size

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def block_number(self) -> int:
        return self._block_number
    
    @property
    def compression_algo(self) -> int:
        return self._compression_algo

    def get_header_size(self) -> int:
        return self.HEADER_FIX_SIZE

    def _valid_header(self) -> None:
        if self._file_size > MAX_BLOCK_NUM * self._block_size:
            raise Exception(
                f"The size of file {self._file_size} is out of the max capability of container ({MAX_BLOCK_NUM} * {self._block_size})."
            )
        if self._version < CURRENT_OLAH_CACHE_VERSION:
            raise Exception(
                f"This Olah Cache file is created by older version Olah. Please remove cache files and retry."
            )

        if self._version > CURRENT_OLAH_CACHE_VERSION:
            raise Exception(
                f"This Olah Cache file is created by newer version Olah. Please remove cache files and retry."
            )

    @staticmethod
    def read(stream) -> "OlahCacheHeader":
        obj = OlahCacheHeader()
        try:
            magic = struct.unpack(
                "<4s", stream.read(4)
            )
        except struct.error:
            raise Exception("File is not a Olah cache file.")
        if magic[0] != OlahCacheHeader.MAGIC_NUMBER:
            raise Exception("File is not a Olah cache file.")
        
        version, block_size, file_size, compression_algo = struct.unpack(
            "<QQQQ", stream.read(OlahCacheHeader.HEADER_FIX_SIZE - 4)
        )
        obj._version = version
        obj._block_size = block_size
        obj._file_size = file_size
        obj._compression_algo = compression_algo
        
        obj._block_number = (file_size + block_size - 1) // block_size

        obj._valid_header()
        return obj

    def write(self, stream):
        btyes_header = struct.pack(
            "<4sQQQQ",
            self.MAGIC_NUMBER,
            self._version,
            self._block_size,
            self._file_size,
            self._compression_algo,
        )
        stream.write(btyes_header)


class OlahCache(object):
    def __init__(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE) -> None:
        self.path: Optional[str] = path
        self.header: Optional[OlahCacheHeader] = None
        self.is_open: bool = False

        # Lock
        self._header_lock = threading.Lock()
        
        # Path
        self._meta_path = os.path.join(path, "meta.bin")
        self._data_path = os.path.join(path, "blocks/block_${block_index}.bin")
        # Incremented when an in-flight download must reset block assembly state (upstream retry).
        self._stream_reset_nonce: int = 0

        self.open(path, block_size=block_size)

    @staticmethod
    def create(path: str, block_size: int = DEFAULT_BLOCK_SIZE):
        return OlahCache(path, block_size=block_size)

    def open(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE):
        if self.is_open:
            raise Exception("This file has been open.")
        if self.path is None:
            raise Exception("The file path is None.")

        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception("The cache path shall be a folder instead of a file.")
            with self._header_lock:
                with portalocker.Lock(self._meta_path, "rb", timeout=60, flags=portalocker.LOCK_SH) as f:
                    f.seek(0)
                    self.header = OlahCacheHeader.read(f)
            self._cleanup_stale_tmp_files()
        else:
            os.makedirs(self.path, exist_ok=True)
            os.makedirs(os.path.join(self.path, "blocks"), exist_ok=True)
            self._cleanup_stale_tmp_files()
            with self._header_lock:
                # Create new file
                with portalocker.Lock(self._meta_path, "wb", timeout=60, flags=portalocker.LOCK_EX) as f:
                    f.seek(0)
                    self.header = OlahCacheHeader(
                        version=CURRENT_OLAH_CACHE_VERSION,
                        block_size=block_size,
                        file_size=0,
                    )
                    self.header.write(f)

        self.is_open = True

    def _cleanup_stale_tmp_files(self) -> None:
        if self.path is None:
            return
        blocks_dir = os.path.join(self.path, "blocks")
        if not os.path.isdir(blocks_dir):
            return
        cutoff = time.time() - STALE_TMP_MAX_AGE_SEC
        for name in os.listdir(blocks_dir):
            if not (name.endswith(".tmp") or name.startswith(".block_")):
                continue
            path = os.path.join(blocks_dir, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    logger.info("Removed stale cache temp file: %s", path)
            except OSError:
                pass

    def _expected_decompressed_len(self, block_index: int) -> int:
        file_size = self._get_file_size()
        block_size = self._get_block_size()
        start = block_index * block_size
        if start >= file_size:
            return 0
        return min(block_size, file_size - start)

    def _looks_like_gzip(self, data: bytes) -> bool:
        return len(data) >= 2 and data[:2] == GZIP_MAGIC

    def close(self):
        if not self.is_open:
            raise Exception("This file has been close.")

        self._flush_header()
        self.path = None
        self.header = None

        self.is_open = False

    def _flush_header(self):
        if self.header is None:
            raise Exception("The header of cache file is None")
        if self.path is None:
            raise Exception("The path of cache file is None")
        with self._header_lock:
            with portalocker.Lock(self._meta_path, "rb+", flags=portalocker.LOCK_EX) as f:
                f.seek(0)
                self.header.write(f)

    def _get_file_size(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        with self._header_lock:
            file_size = self.header.file_size
        return file_size

    def _get_block_number(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        with self._header_lock:
            block_number = self.header.block_number
        return block_number

    def _get_block_size(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        with self._header_lock:
            block_size = self.header.block_size
        return block_size

    def _get_header_size(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        with self._header_lock:
            header_size = self.header.get_header_size()
        return header_size

    def _resize_header(self, block_num: int, file_size: int):
        if self.header is None:
            raise Exception("The header of cache file is None")
        with self._header_lock:
            self.header._block_number = block_num
            self.header._file_size = file_size
            self.header._valid_header()

    def _pad_block(self, raw_block: bytes) -> bytes:
        if len(raw_block) < self._get_block_size():
            block = raw_block + b"\x00" * (self._get_block_size() - len(raw_block))
        else:
            block = raw_block
        return block

    def flush(self):
        if not self.is_open:
            raise Exception("This file has been close.")
        self._flush_header()

    def _block_path(self, block_index: int) -> str:
        return string.Template(self._data_path).substitute(block_index=f"{block_index:0>8}")

    def is_terminal_block(self, block_index: int) -> bool:
        return block_index == self._get_block_number() - 1

    def invalidate_blocks_in_range(self, start_pos: int, end_pos: int, reason: str) -> None:
        if start_pos >= end_pos:
            return
        block_size = self._get_block_size()
        start_block = start_pos // block_size
        end_block = (end_pos - 1) // block_size
        for block_index in range(start_block, end_block + 1):
            self._invalidate_block(block_index, reason)

    def request_stream_reset(self, start_pos: int, end_pos: int, reason: str) -> None:
        self.invalidate_blocks_in_range(start_pos, end_pos, reason)
        self._stream_reset_nonce += 1

    def is_complete(self) -> bool:
        if not self.is_open or self.header is None:
            return False
        for block_index in range(self._get_block_number()):
            if not self.has_block(block_index):
                return False
        return True

    def invalidate_all_blocks(self, reason: str) -> None:
        if self.header is None:
            return
        for block_index in range(self._get_block_number()):
            if self.has_block(block_index):
                self._invalidate_block(block_index, reason)
        self._stream_reset_nonce += 1

    async def content_sha256(self) -> str:
        if not self.is_open or self.header is None:
            raise Exception("Cannot hash a closed cache file.")
        digest = hashlib.sha256()
        for block_index in range(self._get_block_number()):
            if not self.has_block(block_index):
                raise Exception(f"Cache block {block_index} is missing.")
            raw_block = await self.read_block(block_index)
            if raw_block is None:
                raise Exception(f"Cache block {block_index} could not be read.")
            expected_len = self._expected_decompressed_len(block_index)
            digest.update(raw_block[:expected_len])
        return digest.hexdigest()

    def has_block(self, block_index: int) -> bool:
        block_path = self._block_path(block_index)
        if not os.path.exists(block_path):
            return False
        size = os.path.getsize(block_path)
        if size <= 0:
            return False
        if self.header is not None and self.header.compression_algo == 1 and size < len(GZIP_MAGIC):
            return False
        return True

    def _invalidate_block(self, block_index: int, reason: str) -> None:
        block_path = self._block_path(block_index)
        lock_path = block_path + ".write.lock"
        try:
            with portalocker.Lock(lock_path, "w", timeout=10, flags=portalocker.LOCK_EX):
                if os.path.exists(block_path):
                    os.remove(block_path)
                    logger.warning(
                        "Invalidated corrupt cache block %d at %s: %s",
                        block_index,
                        block_path,
                        reason,
                    )
        except portalocker.exceptions.LockException:
            logger.warning(
                "Could not lock block %d for invalidation (%s); removing best-effort",
                block_index,
                reason,
            )
            if os.path.exists(block_path):
                os.remove(block_path)

    async def read_block(self, block_index: int) -> Optional[bytes]:
        if not self.is_open:
            raise Exception("This file has been closed.")

        if self.path is None:
            raise Exception("The path of the cache file is None.")
        
        if block_index >= self._get_block_number():
            raise Exception("Invalid block index.")
        
        if self.header is None:
            raise Exception("The header of cache file is None")

        if not self.has_block(block_index=block_index):
            return None
        
        block_path = self._block_path(block_index)

        with portalocker.Lock(block_path, "rb", timeout=60, flags=portalocker.LOCK_SH):
            async with aiofiles.open(block_path, mode="rb") as f:
                raw_block = await f.read()

        if (
            self.header.compression_algo == 1
            and (len(raw_block) < len(GZIP_MAGIC) or not self._looks_like_gzip(raw_block))
        ):
            self._invalidate_block(block_index, "missing or invalid gzip header")
            return None

        def decompression(block_data: bytes, compression_algo: int):
            if compression_algo == 0:
                return block_data
            elif compression_algo == 1:
                return gzip.decompress(block_data)
            elif compression_algo == 2:
                lzma_dec = lzma.LZMADecompressor()
                return lzma_dec.decompress(block_data)
            else:
                raise Exception("Unsupported compression algorithm.")

        try:
            raw_block = await fastapi.concurrency.run_in_threadpool(
                decompression,
                raw_block,
                self.header.compression_algo,
            )
        except (EOFError, OSError, lzma.LZMAError) as exc:
            self._invalidate_block(block_index, str(exc))
            return None

        expected_len = self._expected_decompressed_len(block_index)
        if expected_len > 0 and len(raw_block) != expected_len:
            self._invalidate_block(
                block_index,
                f"decompressed length {len(raw_block)} != expected {expected_len}",
            )
            return None

        block = self._pad_block(raw_block)
        return block

    async def write_block(
        self, block_index: int, block_bytes: bytes, overwrite: bool = False
    ) -> None:
        if not self.is_open:
            raise Exception("This file has been closed.")
        
        if self.path is None:
            raise Exception("The path of the cache file is None. ")

        if block_index >= self._get_block_number():
            raise Exception("Invalid block index.")
        
        if self.header is None:
            raise Exception("The header of cache file is None")

        if len(block_bytes) != self._get_block_size():
            raise Exception("Block size does not match the cache's block size.")
        
        # Truncation
        if (block_index + 1) * self._get_block_size() > self._get_file_size():
            real_block_bytes = block_bytes[
                : self._get_file_size() - block_index * self._get_block_size()
            ]
        else:
            real_block_bytes = block_bytes

        def compression(block_data: bytes, compression_algo: int):
            if compression_algo == 0:
                return block_data
            elif compression_algo == 1:
                level = int(os.getenv("OLAH_CACHE_GZIP_LEVEL", str(OLAH_CACHE_GZIP_LEVEL)))
                block_data = gzip.compress(block_data, compresslevel=level)
            elif compression_algo == 2:
                lzma_enc = lzma.LZMACompressor()
                block_data = lzma_enc.compress(block_data)
            else:
                raise Exception("Unsupported compression algorithm.")
            return block_data

        # Run in the default thread pool executor
        real_block_bytes = await fastapi.concurrency.run_in_threadpool(
            compression,
            real_block_bytes,
            self.header.compression_algo
        )
   
        block_path = self._block_path(block_index)
        block_dir = os.path.dirname(block_path)
        lock_path = block_path + ".write.lock"

        with portalocker.Lock(lock_path, "w", timeout=120, flags=portalocker.LOCK_EX):
            if not overwrite and self.has_block(block_index):
                return
            if overwrite and self.has_block(block_index):
                block_path_existing = self._block_path(block_index)
                if os.path.exists(block_path_existing):
                    os.remove(block_path_existing)

            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=block_dir,
                prefix=f".block_{block_index:0>8}_",
                suffix=".tmp",
                delete=False,
            ) as tmp:
                tmp_path = tmp.name

            try:
                async with aiofiles.open(tmp_path, mode="wb") as f:
                    await f.write(real_block_bytes)
                    await f.flush()
                await fastapi.concurrency.run_in_threadpool(self._fsync_path, tmp_path)
                os.replace(tmp_path, block_path)
                await fastapi.concurrency.run_in_threadpool(self._fsync_dir, block_dir)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        self._flush_header()

    @staticmethod
    def _fsync_path(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDWR)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            # Best-effort durability; some platforms/temp dirs may not support fsync.
            pass

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            if hasattr(os, "O_DIRECTORY"):
                fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            else:
                fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    def _resize_file_size(self, file_size: int):
        """
        Deprecation
        """
        if not self.is_open:
            raise Exception("This file has been closed.")
        
        if self.path is None:
            raise Exception("The path of the cache file is None. ")

        if file_size == self._get_file_size():
            return
        if file_size < self._get_file_size():
            raise Exception(
                "Invalid resize file size. New file size must be greater than the current file size."
            )

        with open(self.path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED, mmap.PROT_READ) as mm:
                mm.seek(0, os.SEEK_END)
                bin_size = mm.tell()

        # FIXME: limit the resize method, because it may influence the _block_mask
        new_bin_size = self._get_header_size() + file_size
        with open(self.path, "rb+") as f:
            with mmap.mmap(f.fileno(), 0, mmap.MAP_SHARED, mmap.PROT_WRITE) as mm:
                mm.seek(new_bin_size - 1)
                mm.write(b'\0')
                mm.truncate()
                
                # Extend file size (slow)
                # mm.seek(0, os.SEEK_END)
                # mm.write(b"\x00" * (new_bin_size - bin_size))

    def resize(self, file_size: int):
        """
        Deprecation
        """
        if not self.is_open:
            raise Exception("This file has been closed.")
        bs = self._get_block_size()
        new_block_num = (file_size + bs - 1) // bs
        self._resize_header(new_block_num, file_size)
        self._flush_header()
