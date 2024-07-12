from io import BufferedReader
import os
import struct
from typing import BinaryIO, Optional
from .bitset import Bitset

CURRENT_OLAH_CACHE_VERSION = 8
DEFAULT_BLOCK_MASK_MAX = 1024 * 1024
DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024


class OlahCacheHeader(object):
    MAGIC_NUMBER = "OLAH".encode("ascii")
    HEADER_FIX_SIZE = 36

    def __init__(
        self,
        version: int = CURRENT_OLAH_CACHE_VERSION,
        block_size: int = DEFAULT_BLOCK_SIZE,
        file_size: int = 0,
    ) -> None:
        self.version = version
        self.block_size = block_size

        self.file_size = file_size
        self.block_number = (file_size + block_size - 1) // block_size

        self.block_mask_size = DEFAULT_BLOCK_MASK_MAX
        self.block_mask = Bitset(DEFAULT_BLOCK_MASK_MAX)
    
    def get_header_size(self):
        return self.HEADER_FIX_SIZE + len(self.block_mask.bits)

    def _valid_header(self):
        if self.file_size > self.block_mask_size * self.block_size:
            raise Exception(
                f"The size of file {self.file_size} is out of the max capability of container ({self.block_mask_size} * {self.block_size})."
            )

    @staticmethod
    def read(stream) -> "OlahCacheHeader":
        obj = OlahCacheHeader()
        magic, version, block_size, file_size, block_mask_size = struct.unpack(
            "<4sQQQQ", stream.read(OlahCacheHeader.HEADER_FIX_SIZE)
        )
        if magic != OlahCacheHeader.MAGIC_NUMBER:
            raise Exception("The file is not a valid olah cache file.")
        obj.version = version
        obj.block_size = block_size
        obj.file_size = file_size
        obj.block_number = (file_size + block_size - 1) // block_size
        obj.block_mask_size = block_mask_size
        obj.block_mask = Bitset(block_mask_size)
        obj.block_mask.bits = bytearray(stream.read((block_mask_size + 7) // 8))

        obj._valid_header()
        return obj

    def write(self, stream):
        btyes_header = struct.pack(
            "<4sQQQQ",
            self.MAGIC_NUMBER,
            self.version,
            self.block_size,
            self.file_size,
            self.block_mask_size,
        )
        btyes_out = btyes_header + self.block_mask.bits
        stream.write(btyes_out)

class OlahCache(object):
    def __init__(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE) -> None:
        self.path: Optional[str] = path
        self.header: Optional[OlahCacheHeader] = None
        self.is_open: bool = False
        self.open(path, block_size=block_size)

    @staticmethod
    def create(path: str, block_size: int = DEFAULT_BLOCK_SIZE):
        return OlahCache(path, block_size=block_size)

    def open(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE):
        if self.is_open:
            raise Exception("This file has been open.")
        if os.path.exists(path):
            with open(path, "rb") as f:
                f.seek(0)
                self.header = OlahCacheHeader.read(f)
        else:
            # Create new file
            with open(path, "wb") as f:
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
        with open(self.path, "rb+") as f:
            f.seek(0)
            self.header.write(f)

    def flush(self):
        if not self.is_open:
            raise Exception("This file has been close.")
        self._flush_header()
    
    def _has_block(self, block_index: int) -> bool:
        return self.header.block_mask.test(block_index)

    def _read_block(self, block_index: int) -> Optional[bytes]:
        if not self.is_open:
            raise Exception("This file has been closed.")

        if block_index >= self.header.block_number:
            raise Exception("Invalid block index.")
        
        if not self._has_block(block_index=block_index):
            return None

        offset = self.header.get_header_size() + (block_index * self.header.block_size)
        with open(self.path, "rb") as f:
            f.seek(offset)
            return f.read(self.header.block_size)
    
    def _write_block(self, block_index: int, block_bytes: bytes) -> None:
        if not self.is_open:
            raise Exception("This file has been closed.")

        if block_index >= self.header.block_number:
            raise Exception("Invalid block index.")
        
        if len(block_bytes) != self.header.block_size:
            raise Exception("Block size does not match the cache's block size.")

        offset = self.header.get_header_size() + (block_index * self.header.block_size)
        with open(self.path, "rb+") as f:
            f.seek(offset)
            f.write(block_bytes)
        
        self.header.block_mask.set(block_index)
    
    def _resize_blocks(self, block_num: int):
        if not self.is_open:
            raise Exception("This file has been closed.")
        if block_num == self.header.block_number:
            return
        if block_num <= self.header.block_number:
            raise Exception("Invalid block number. New block number must be greater than the current block number.")

        with open(self.path, "rb") as f:
            f.seek(0, os.SEEK_END)
            bin_size = f.tell()

        new_bin_size = self.header.get_header_size() + block_num * self.header.block_size
        with open(self.path, "rb+") as f:
            # Extend file size
            f.seek(0, os.SEEK_END)
            f.write(b'\x00' * (new_bin_size - bin_size))

    def resize(self, file_size: int):
        if not self.is_open:
            raise Exception("This file has been closed.")
        new_block_num = (file_size + self.header.block_size - 1) // self.header.block_size
        self._resize_blocks(new_block_num)

        self.header.block_number = new_block_num
        self.header.file_size = file_size
        self.header._valid_header()
        self._flush_header()
         