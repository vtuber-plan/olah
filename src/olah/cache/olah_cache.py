# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import asyncio
import lzma
import mmap
import os
import string
import struct
import threading
import gzip
from typing import BinaryIO, Dict, List, Optional

import aiofiles
import fastapi
import fastapi.concurrency
import portalocker
from .bitset import Bitset

CURRENT_OLAH_CACHE_VERSION = 9
# Due to the download chunk settings: https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/constants.py#L37
DEFAULT_BLOCK_SIZE = 50 * 1024 * 1024
MAX_BLOCK_NUM = 8192
DEFAULT_COMPRESSION_ALGO = 1
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
        else:
            os.makedirs(self.path, exist_ok=True)
            os.makedirs(os.path.join(self.path, "blocks"), exist_ok=True)
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

    def has_block(self, block_index: int) -> bool:
        block_path = string.Template(self._data_path).substitute(block_index=f"{block_index:0>8}")
        return os.path.exists(block_path)

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
        
        block_path = string.Template(self._data_path).substitute(block_index=f"{block_index:0>8}")

        with portalocker.Lock(block_path, "rb", timeout=60, flags=portalocker.LOCK_SH) as fh:
            async with aiofiles.open(block_path, mode='rb') as f:
                raw_block = await f.read(self._get_block_size())
        
        def decompression(block_data: bytes, compression_algo: int):
            # compression
            if compression_algo == 0:
                return block_data
            elif compression_algo == 1:
                block_data = gzip.decompress(block_data)
            elif compression_algo == 2:
                lzma_dec = lzma.LZMADecompressor()
                block_data = lzma_dec.decompress(block_data)
            else:
                raise Exception("Unsupported compression algorithm.")
            return block_data

        raw_block = await fastapi.concurrency.run_in_threadpool(
            decompression,
            raw_block,
            self.header.compression_algo
        )

        block = self._pad_block(raw_block)
        return block

    async def write_block(self, block_index: int, block_bytes: bytes) -> None:
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
                block_data = gzip.compress(block_data, compresslevel=4)
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
   
        block_path = string.Template(self._data_path).substitute(block_index=f"{block_index:0>8}")

        with portalocker.Lock(block_path, 'wb+', timeout=60, flags=portalocker.LOCK_EX) as fh:
            async with aiofiles.open(block_path, mode='wb+') as f:
                await f.write(real_block_bytes)

        self._flush_header()

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
