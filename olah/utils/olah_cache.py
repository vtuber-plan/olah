from io import BufferedReader
import os
import struct
import threading
from typing import BinaryIO, Optional
from .bitset import Bitset

CURRENT_OLAH_CACHE_VERSION = 8
DEFAULT_BLOCK_MASK_MAX = 1024 * 1024
DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024


class OlahCacheHeader(object):
    MAGIC_NUMBER = "OLAH".encode("ascii")
    HEADER_FIX_SIZE = 36

    def __init__(
        self,
        version: int = CURRENT_OLAH_CACHE_VERSION,
        block_size: int = DEFAULT_BLOCK_SIZE,
        file_size: int = 0,
    ) -> None:
        self._version = version
        self._block_size = block_size

        self._file_size = file_size
        self._block_number = (file_size + block_size - 1) // block_size

        self._block_mask_size = DEFAULT_BLOCK_MASK_MAX
        self._block_mask = Bitset(DEFAULT_BLOCK_MASK_MAX)
    
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
    def block_mask(self) -> int:
        return self._block_mask

    def get_header_size(self):
        return self.HEADER_FIX_SIZE + len(self._block_mask.bits)

    def _valid_header(self):
        if self._file_size > self._block_mask_size * self._block_size:
            raise Exception(
                f"The size of file {self._file_size} is out of the max capability of container ({self._block_mask_size} * {self._block_size})."
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
        magic, version, block_size, file_size, block_mask_size = struct.unpack(
            "<4sQQQQ", stream.read(OlahCacheHeader.HEADER_FIX_SIZE)
        )
        if magic != OlahCacheHeader.MAGIC_NUMBER:
            raise Exception("The file is not a valid olah cache file.")
        obj._version = version
        obj._block_size = block_size
        obj._file_size = file_size
        obj._block_number = (file_size + block_size - 1) // block_size
        obj._block_mask_size = block_mask_size
        obj._block_mask = Bitset(block_mask_size)
        obj._block_mask.bits = bytearray(stream.read((block_mask_size + 7) // 8))

        obj._valid_header()
        return obj

    def write(self, stream):
        btyes_header = struct.pack(
            "<4sQQQQ",
            self.MAGIC_NUMBER,
            self._version,
            self._block_size,
            self._file_size,
            self._block_mask_size,
        )
        btyes_out = btyes_header + self._block_mask.bits
        stream.write(btyes_out)

class OlahCache(object):
    def __init__(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE) -> None:
        self.path: Optional[str] = path
        self.header: Optional[OlahCacheHeader] = None
        self.is_open: bool = False

        self.header_lock = threading.Lock()

        self.open(path, block_size=block_size)
        

    @staticmethod
    def create(path: str, block_size: int = DEFAULT_BLOCK_SIZE):
        return OlahCache(path, block_size=block_size)

    def open(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE):
        if self.is_open:
            raise Exception("This file has been open.")
        if os.path.exists(path):
            with self.header_lock:
                with open(path, "rb") as f:
                    f.seek(0)
                    self.header = OlahCacheHeader.read(f)
        else:
            with self.header_lock:
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
        with self.header_lock:
            with open(self.path, "rb+") as f:
                f.seek(0)
                self.header.write(f)

    def _get_file_size(self) -> int:
        with self.header_lock:
            file_size = self.header.file_size
        return file_size

    def _get_block_number(self) -> int:
        with self.header_lock:
            block_number = self.header.block_number
        return block_number
    
    def _get_block_size(self) -> int:
        with self.header_lock:
            block_size = self.header.block_size
        return block_size
    
    def _get_header_size(self) -> int:
        with self.header_lock:
            header_size = self.header.get_header_size()
        return header_size
    
    def _resize_header(self, block_num: int, file_size: int):
        with self.header_lock:
            self.header._block_number = block_num
            self.header._file_size = file_size
            self.header._valid_header()
    
    def _set_header_block(self, block_index: int):
        with self.header_lock:
            self.header.block_mask.set(block_index)

    def _test_header_block(self, block_index: int):
        with self.header_lock:
            self.header.block_mask.test(block_index)

    def flush(self):
        if not self.is_open:
            raise Exception("This file has been close.")
        self._flush_header()
    
    def has_block(self, block_index: int) -> bool:
        self._test_header_block(block_index)

    def read_block(self, block_index: int) -> Optional[bytes]:
        if not self.is_open:
            raise Exception("This file has been closed.")

        if block_index >= self._get_block_number():
            raise Exception("Invalid block index.")
    
        if not self.has_block(block_index=block_index):
            return None

        offset = self._get_header_size() + (block_index * self._get_block_size())
        with open(self.path, "rb") as f:
            f.seek(offset)
            raw_block = f.read(self._get_block_size())
        if len(raw_block) < self._get_block_size():
            block = raw_block + b"\x00" * (self._get_block_size() - len(raw_block))
        else:
            block = raw_block
        return block
    
    def write_block(self, block_index: int, block_bytes: bytes) -> None:
        if not self.is_open:
            raise Exception("This file has been closed.")

        if block_index >= self._get_block_number():
            raise Exception("Invalid block index.")

        if len(block_bytes) != self._get_block_size():
            raise Exception("Block size does not match the cache's block size.")

        offset = self._get_header_size() + (block_index * self._get_block_size())
        with open(self.path, "rb+") as f:
            f.seek(offset)
            if (block_index + 1) * self._get_block_size() > self._get_file_size():
                real_block_bytes = block_bytes[:self._get_file_size() - block_index * self._get_block_size()]
            else:
                real_block_bytes = block_bytes
            f.write(real_block_bytes)
        
        self._set_header_block(block_index)
        self._flush_header()
    
    def _resize_file_size(self, file_size: int):
        if not self.is_open:
            raise Exception("This file has been closed.")
        if file_size == self._get_file_size():
            return
        if file_size < self._get_file_size():
            raise Exception("Invalid resize file size. New file size must be greater than the current file size.")

        with open(self.path, "rb") as f:
            f.seek(0, os.SEEK_END)
            bin_size = f.tell()

        new_bin_size = self._get_header_size() + file_size
        with open(self.path, "rb+") as f:
            # Extend file size
            f.seek(0, os.SEEK_END)
            f.write(b'\x00' * (new_bin_size - bin_size))

    def resize(self, file_size: int):
        if not self.is_open:
            raise Exception("This file has been closed.")
        bs = self._get_block_size()
        new_block_num = (file_size + bs - 1) // bs
        self._resize_file_size(file_size)
        self._resize_header(new_block_num, file_size)
        self._flush_header()
         