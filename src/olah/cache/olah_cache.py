# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""On-disk block cache ("v10" format).

A cache entry is a directory::

    <save_path>/
        meta.bin        # versioned header (magic + sizes + etag + extension TLV + CRC32)
        chunks.crc      # flat u32[] of CRC32 per 1 MiB chunk of the *logical* payload
        blocks/
            00000000.bin
            00000001.bin
            ...

Design notes
------------
* **Identity** -- ``meta.bin`` stores the content etag; ``OlahCache.open`` takes an
  ``expected_etag`` and revalidates (online only; offline trusts the disk).
* **Integrity** -- every 1 MiB chunk has a CRC32 in ``chunks.crc``; reads verify the
  chunks they serve and never return unverified-corrupt bytes (a mismatch drops the
  block so it is re-fetched).
* **No cross-version compatibility** -- on any version / CRC / etag mismatch the cache
  directory is wiped and recreated cleanly. A TLV extension region lets most future
  metadata changes be *additive* (same version) so version bumps stay rare.
* **chunk == 1 MiB** unifies the streaming yield size, the integrity unit, and the
  compression sub-chunk size. Uncompressed blocks stay raw contiguous payload (raw
  seek+read); compressed blocks carry a sub-chunk index so a range read decompresses
  only the overlapping sub-chunks (bounded memory).
"""

import lzma
import os
import struct
import threading
import zlib
from typing import AsyncIterator, BinaryIO, Dict, List, Optional, Union

import fastapi.concurrency
import portalocker

from olah.cache.bitset import Bitset

CURRENT_OLAH_CACHE_VERSION = 10

# Block / chunk geometry. The 1 MiB chunk is the single unit for streaming,
# integrity, and compression; block_size is kept a multiple of chunk_size so
# blocks always align to chunk boundaries.
DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024
DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024
MAX_BLOCK_NUM = 131072  # 64 MiB * 131072 == 8 TiB ceiling per file

DEFAULT_COMPRESSION_ALGO = 0
"""
0: no compression
1: gzip  (stored as independently-decompressible raw-deflate sub-chunks)
2: lzma  (stored as independently-decompressible raw lzma2 sub-chunks)
"""

# Maps the user-facing config name (see OlahConfig.cache_compression) to the
# numeric algorithm stored in each cache's meta.bin header.
COMPRESSION_NAME_TO_ALGO: Dict[str, int] = {
    "none": 0,
    "gzip": 1,
    "lzma": 2,
}

# Pinned lzma filter spec for compressed sub-chunks. Raw lzma streams cannot be
# decoded without the exact filter list, so it is fixed here and identified by
# filter_id 0 in the block header.
_LZMA_RAW_FILTERS = [{"id": lzma.FILTER_LZMA2, "preset": 4}]


def compression_algo_from_name(name: str) -> int:
    """Resolve a compression config name to its numeric algorithm id."""
    if name not in COMPRESSION_NAME_TO_ALGO:
        raise ValueError(
            f"Unknown compression algorithm: {name}. "
            f"Expected one of {list(COMPRESSION_NAME_TO_ALGO.keys())}."
        )
    return COMPRESSION_NAME_TO_ALGO[name]


class CacheIntegrityError(Exception):
    """Raised when a cache block fails CRC verification (bit-rot / torn write)."""


# ---------------------------------------------------------------------------
# Sub-chunk (de)compression helpers
# ---------------------------------------------------------------------------

def _compress_chunk(block_data: bytes, compression_algo: int) -> bytes:
    """Compress a single <= chunk_size piece into an independently-decompressible stream."""
    if compression_algo == 1:
        # Raw deflate (no gzip/zlib framing): each chunk is a standalone stream.
        co = zlib.compressobj(level=4, method=zlib.DEFLATED, wbits=-15)
        return co.compress(block_data) + co.flush(zlib.Z_FINISH)
    elif compression_algo == 2:
        co = lzma.LZMACompressor(format=lzma.FORMAT_RAW, filters=_LZMA_RAW_FILTERS)
        return co.compress(block_data) + co.flush()
    raise Exception("Unsupported compression algorithm.")


def _decompress_chunk(stream: bytes, compression_algo: int) -> bytes:
    """Decompress a single independently-decompressible sub-chunk."""
    if compression_algo == 1:
        return zlib.decompress(stream, wbits=-15)
    elif compression_algo == 2:
        dec = lzma.LZMADecompressor(format=lzma.FORMAT_RAW, filters=_LZMA_RAW_FILTERS)
        return dec.decompress(stream)
    raise Exception("Unsupported compression algorithm.")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

class OlahCacheHeader(object):
    MAGIC_NUMBER = b"OLAH"
    # magic(4s) version(H) flags(H) header_size(I) block_size(Q) file_size(Q)
    # compression_algo(B) chunk_size(I) etag_len(H) reserved(8s)
    FIXED_PREFIX_FMT = "<4sHHIQQBIH8s"
    FIXED_PREFIX_SIZE = struct.calcsize(FIXED_PREFIX_FMT)  # 43

    FLAG_HAS_ETAG = 1 << 0  # informational; etag presence is also implied by etag_len > 0

    def __init__(
        self,
        version: int = CURRENT_OLAH_CACHE_VERSION,
        flags: int = 0,
        block_size: int = DEFAULT_BLOCK_SIZE,
        file_size: int = 0,
        compression_algo: int = DEFAULT_COMPRESSION_ALGO,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        etag: Union[bytes, str, None] = None,
        extension: bytes = b"",
    ) -> None:
        self._version = version
        self._flags = flags
        self._block_size = block_size
        self._file_size = file_size
        self._compression_algo = compression_algo
        self._chunk_size = chunk_size
        self._etag = self._normalize_etag(etag)
        self._extension = bytes(extension or b"")

    @staticmethod
    def _normalize_etag(etag: Union[bytes, str, None]) -> bytes:
        if etag is None:
            return b""
        if isinstance(etag, str):
            return etag.encode("utf-8")
        return bytes(etag)

    # --- properties --------------------------------------------------------

    @property
    def version(self) -> int:
        return self._version

    @property
    def flags(self) -> int:
        return self._flags

    @property
    def block_size(self) -> int:
        return self._block_size

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def compression_algo(self) -> int:
        return self._compression_algo

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def etag(self) -> bytes:
        return self._etag

    @property
    def extension(self) -> bytes:
        return self._extension

    @property
    def block_number(self) -> int:
        return (self._file_size + self._block_size - 1) // self._block_size

    @property
    def chunk_number(self) -> int:
        return (self._file_size + self._chunk_size - 1) // self._chunk_size

    def chunks_per_block(self) -> int:
        return self._block_size // self._chunk_size

    def get_header_size(self) -> int:
        return (
            self.FIXED_PREFIX_SIZE
            + len(self._etag)
            + 4  # extension_len
            + len(self._extension)
            + 4  # trailing crc32
        )

    # --- validation --------------------------------------------------------

    def _validate(self) -> None:
        if self._chunk_size <= 0:
            raise Exception(f"Invalid chunk_size: {self._chunk_size}.")
        if self._block_size % self._chunk_size != 0:
            raise Exception(
                f"block_size ({self._block_size}) must be a multiple of "
                f"chunk_size ({self._chunk_size})."
            )
        if self._file_size > MAX_BLOCK_NUM * self._block_size:
            raise Exception(
                f"The size of file {self._file_size} is out of the max capability of "
                f"container ({MAX_BLOCK_NUM} * {self._block_size})."
            )
        if len(self._etag) > 0xFFFF:
            raise Exception(f"etag too long ({len(self._etag)} > 65535).")
        if self._compression_algo not in (0, 1, 2):
            raise Exception(f"Unsupported compression algorithm: {self._compression_algo}.")

    # --- (de)serialization -------------------------------------------------

    @classmethod
    def read(cls, stream: BinaryIO) -> "OlahCacheHeader":
        prefix = stream.read(cls.FIXED_PREFIX_SIZE)
        if len(prefix) < cls.FIXED_PREFIX_SIZE:
            raise Exception("File is not a Olah cache file (truncated header).")
        (
            magic,
            version,
            flags,
            header_size,
            block_size,
            file_size,
            compression_algo,
            chunk_size,
            etag_len,
            _reserved,
        ) = struct.unpack(cls.FIXED_PREFIX_FMT, prefix)
        if magic != cls.MAGIC_NUMBER:
            raise Exception("File is not a Olah cache file.")
        # header_size is the whole file (prefix + etag + extension_len +
        # extension + crc32); the trailing 4 bytes are the CRC.
        if header_size < cls.FIXED_PREFIX_SIZE + 4 + 4:
            raise Exception("Corrupt cache header (header_size too small).")

        rest_len = header_size - cls.FIXED_PREFIX_SIZE
        rest = stream.read(rest_len)
        if len(rest) < rest_len:
            raise Exception("Corrupt cache header (truncated).")

        stored_crc = struct.unpack("<I", rest[-4:])[0]
        body = rest[:-4]  # etag + extension_len(u32) + extension
        actual_crc = zlib.crc32(prefix + body) & 0xFFFFFFFF
        if actual_crc != stored_crc:
            raise Exception("Corrupt cache header (CRC mismatch).")

        if etag_len > len(body):
            raise Exception("Corrupt cache header (etag length inconsistent).")
        etag = body[:etag_len]
        if len(body) < etag_len + 4:
            raise Exception("Corrupt cache header (missing extension length).")
        (extension_len,) = struct.unpack("<I", body[etag_len:etag_len + 4])
        max_extension = len(body) - etag_len - 4
        if extension_len > max_extension:
            raise Exception("Corrupt cache header (extension length out of bounds).")
        extension = body[etag_len + 4:etag_len + 4 + extension_len]

        obj = cls(
            version=version,
            flags=flags,
            block_size=block_size,
            file_size=file_size,
            compression_algo=compression_algo,
            chunk_size=chunk_size,
            etag=etag,
            extension=extension,
        )
        obj._validate()
        return obj

    def write(self, stream: BinaryIO) -> None:
        self._validate()
        etag = self._etag
        flags = self._flags | (self.FLAG_HAS_ETAG if etag else 0)
        header_size = self.get_header_size()
        prefix = struct.pack(
            self.FIXED_PREFIX_FMT,
            self.MAGIC_NUMBER,
            self._version,
            flags,
            header_size,
            self._block_size,
            self._file_size,
            self._compression_algo,
            self._chunk_size,
            len(etag),
            b"\x00" * 8,
        )
        body = etag + struct.pack("<I", len(self._extension)) + self._extension
        crc = zlib.crc32(prefix + body) & 0xFFFFFFFF
        stream.write(prefix + body + struct.pack("<I", crc))


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class OlahCache(object):
    def __init__(
        self,
        path: str,
        file_size: int = 0,
        block_size: int = DEFAULT_BLOCK_SIZE,
        compression_algo: int = DEFAULT_COMPRESSION_ALGO,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        expected_etag: Union[bytes, str, None] = None,
    ) -> None:
        self.path: Optional[str] = path
        self.header: Optional[OlahCacheHeader] = None
        self.is_open: bool = False

        # Whether meta.bin has in-memory changes not yet persisted to disk.
        self._header_dirty: bool = False

        # In-process lock serializing header mutations.
        self._header_lock = threading.Lock()

        # Paths
        self._meta_path = os.path.join(path, "meta.bin")
        # Sidecar advisory-lock target for the meta lifecycle (open/revalidate/
        # wipe/create/resize). Content is NEVER read or written -- the file exists
        # only so portalocker has a stable inode to lock. Keeping it separate from
        # meta.bin avoids the POSIX/NFS pitfall where closing any fd on an inode
        # releases all locks the process holds on that inode.
        self._meta_lock_path = os.path.join(path, "meta.lock")
        self._crc_path = os.path.join(path, "chunks.crc")
        self._blocks_dir = os.path.join(path, "blocks")

        # In-memory presence bitmap (revives cache/bitset.py). Built once per
        # open() from a single blocks/ scan so bulk iterators (is_fully_cached,
        # get_contiguous_ranges) avoid stat-ing every block. has_block() stays
        # disk-authoritative for single-block decisions.
        self._present: Optional[Bitset] = None

        self.open(
            path,
            file_size=file_size,
            block_size=block_size,
            compression_algo=compression_algo,
            chunk_size=chunk_size,
            expected_etag=expected_etag,
        )

    @staticmethod
    def create(
        path: str,
        file_size: int = 0,
        block_size: int = DEFAULT_BLOCK_SIZE,
        compression_algo: int = DEFAULT_COMPRESSION_ALGO,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        expected_etag: Union[bytes, str, None] = None,
    ):
        return OlahCache(
            path,
            file_size=file_size,
            block_size=block_size,
            compression_algo=compression_algo,
            chunk_size=chunk_size,
            expected_etag=expected_etag,
        )

    # --- lifecycle ---------------------------------------------------------

    def open(
        self,
        path: str,
        file_size: int = 0,
        block_size: int = DEFAULT_BLOCK_SIZE,
        compression_algo: int = DEFAULT_COMPRESSION_ALGO,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        expected_etag: Union[bytes, str, None] = None,
    ):
        if self.is_open:
            raise Exception("This file has been open.")
        if self.path is None:
            raise Exception("The file path is None.")

        os.makedirs(self.path, exist_ok=True)
        os.makedirs(self._blocks_dir, exist_ok=True)

        expected_etag_bytes = OlahCacheHeader._normalize_etag(expected_etag)
        self._ensure_meta_lock()

        def _read_and_validate() -> OlahCacheHeader:
            """Read meta.bin fresh and validate identity. Returns the header or raises.

            Online revalidation: a non-None ``expected_etag`` that differs means the
            upstream content changed. Offline callers pass ``None`` to trust the disk
            and never destroy a cache while offline.
            """
            with open(self._meta_path, "rb") as f:
                candidate = OlahCacheHeader.read(f)
            if candidate.version != CURRENT_OLAH_CACHE_VERSION:
                raise Exception("Cache version mismatch; recreating.")
            if expected_etag_bytes and candidate.etag != expected_etag_bytes:
                raise Exception("Cache etag mismatch; recreating.")
            return candidate

        # Readers-writer lifecycle lock on the sidecar meta.lock:
        #   * LOCK_SH to read+validate -- concurrent cache-hit opens don't serialize.
        #   * LOCK_EX to wipe+create -- mutually exclusive with readers and other
        #     creators, so no reader ever observes a half-formed cache.
        # The SH->release->EX window is closed by a double-checked re-read under EX.
        reused = False
        try:
            with portalocker.Lock(self._meta_lock_path, "a+", flags=portalocker.LOCK_SH):
                if os.path.exists(self._meta_path):
                    try:
                        self.header = _read_and_validate()
                        reused = True
                    except Exception:
                        reused = False
        except Exception:
            reused = False

        if not reused:
            with portalocker.Lock(self._meta_lock_path, "a+", flags=portalocker.LOCK_EX):
                # Double-check under EX: another worker may have created a valid
                # cache while we waited for the exclusive lock.
                if os.path.exists(self._meta_path):
                    try:
                        self.header = _read_and_validate()
                        reused = True
                    except Exception:
                        reused = False
                if not reused:
                    self._wipe()
                    self.header = OlahCacheHeader(
                        version=CURRENT_OLAH_CACHE_VERSION,
                        block_size=block_size,
                        file_size=file_size,
                        compression_algo=compression_algo,
                        chunk_size=chunk_size,
                        etag=expected_etag_bytes,
                    )
                    self._header_dirty = True
                    self._flush_header()
                    self._resize_crc_file()

        self.is_open = True
        self._build_present_map()
        self._touch_access()

    def _ensure_meta_lock(self) -> None:
        """Create the sidecar meta.lock if absent. Used only as a lock target.

        ``"a+"`` creates the file when missing and never truncates an existing one,
        so concurrent creators racing to create it are both fine (idempotent).
        """
        if os.path.exists(self._meta_lock_path):
            return
        with open(self._meta_lock_path, "a+"):
            pass
        self._fsync_dir(self.path)

    def _touch_access(self) -> None:
        """Bump meta.lock mtime as a reliable last-access marker for LRU eviction.

        FS atime is unreliable under noatime/relatime, and the old code touched
        the wrong inode anyway (a directory, not the block files). mtime is always
        maintained by the FS and meta.lock is never content-read, so its mtime is
        an unambiguous per-entry access signal that eviction sorts on.
        """
        try:
            os.utime(self._meta_lock_path, None)
        except OSError:
            pass

    def close(self):
        if not self.is_open:
            raise Exception("This file has been close.")
        if self._header_dirty:
            self._flush_header()
        self.path = None
        self.header = None
        self.is_open = False

    # --- header persistence ------------------------------------------------

    def _flush_header(self):
        if self.header is None:
            raise Exception("The header of cache file is None")
        if self.path is None:
            raise Exception("The path of cache file is None")
        with self._header_lock:
            # Atomic rewrite: write a unique temp file, fsync, rename. meta.bin
            # is unrecoverable if torn, so it is never rewritten in place.
            tmp_path = os.path.join(
                self.path, f".meta.{os.getpid()}.{threading.get_ident()}.tmp"
            )
            with open(tmp_path, "wb") as f:
                self.header.write(f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._meta_path)
            self._fsync_dir(self.path)
            self._header_dirty = False

    def _wipe(self):
        """Remove meta.bin / chunks.crc / block files so the cache is recreated fresh.

        Never removes the sidecar ``meta.lock`` or any ``.dl.lock`` / ``.tmp``
        files: lock files are coordination primitives held by other processes, and
        yanking one out from under a holder breaks single-flight / lifecycle
        mutual exclusion.
        """
        if self.path is None:
            return
        # The bitmap is stale once blocks/ is cleared; open() rebuilds it after
        # recreation.
        self._present = None
        for p in (self._meta_path, self._crc_path):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
        if os.path.isdir(self._blocks_dir):
            for name in os.listdir(self._blocks_dir):
                if name.endswith(".dl.lock") or name.endswith(".tmp"):
                    continue
                try:
                    os.remove(os.path.join(self._blocks_dir, name))
                except FileNotFoundError:
                    pass

    # --- accessors ---------------------------------------------------------

    def _get_file_size(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        return self.header.file_size

    def _get_block_number(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        return self.header.block_number

    def _get_block_size(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        return self.header.block_size

    def _get_chunk_size(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        return self.header.chunk_size

    def _get_compression_algo(self) -> int:
        if self.header is None:
            raise Exception("The header of cache file is None")
        return self.header.compression_algo

    def _resize_header(self, file_size: int):
        if self.header is None:
            raise Exception("The header of cache file is None")
        with self._header_lock:
            self.header._file_size = file_size
            self.header._validate()
            self._header_dirty = True

    def flush(self):
        if not self.is_open:
            raise Exception("This file has been close.")
        if self._header_dirty:
            self._flush_header()

    def resize(self, file_size: int):
        if not self.is_open:
            raise Exception("This file has been closed.")
        if file_size == self._get_file_size():
            return
        # Serialize header mutation + crc resize against other workers so a
        # concurrent resize cannot interleave a header write with a crc resize
        # (which would leave chunks.crc sized for a different file_size).
        with portalocker.Lock(self._meta_lock_path, "a+", flags=portalocker.LOCK_EX):
            self._resize_header(file_size)
            self._flush_header()
            self._resize_crc_file()
        # file_size (and thus block count) changed; rebuild the presence bitmap.
        self._build_present_map()

    # --- block presence ----------------------------------------------------

    def has_block(self, block_index: int) -> bool:
        """Disk-authoritative presence check (exists + non-zero size).

        Used for single-block decisions where cross-process correctness matters
        (read pre-check, single-flight double-check). Bulk iterators should use
        is_block_cached(), which consults the in-memory bitmap.
        """
        block_path = self.get_block_path(block_index)
        return os.path.exists(block_path) and os.path.getsize(block_path) > 0

    def is_block_cached(self, block_index: int) -> bool:
        """Bitmap-backed O(1) presence check for bulk iteration.

        Best-effort: reflects blocks/ at open() time plus this instance's own
        writes/invalidations. A stale miss only ever costs a redundant
        single-flight download (has_block still gates reads/writes), never
        corruption. Falls back to has_block if the bitmap is unavailable.
        """
        if self._present is None:
            return self.has_block(block_index)
        try:
            return self._present.test(block_index)
        except IndexError:
            return False

    def is_fully_cached(self) -> bool:
        """True if every block of the file is present on disk.

        Uses the in-memory bitmap when available (one scandir at open, not a stat
        per block). A zero-size file is treated as NOT fully cached so callers
        fall through to the resolve path.
        """
        if self.header is None:
            return False
        n = self._get_block_number()
        if n == 0:
            return False
        if self._present is not None:
            return all(self._present.test(b) for b in range(n))
        return all(self.has_block(b) for b in range(n))

    def _build_present_map(self) -> None:
        """Populate ``self._present`` from a single blocks/ scan.

        Turns the bulk-iterator hot path (is_fully_cached / get_contiguous_ranges
        over thousands of blocks) from one stat per block into one scandir.
        Re-called after resize, since file_size changes the block count.
        """
        if self.header is None:
            self._present = None
            return
        n = self.header.block_number
        bs = Bitset(n if n > 0 else 1)
        try:
            with os.scandir(self._blocks_dir) as it:
                for entry in it:
                    if not entry.is_file():
                        continue
                    name = entry.name
                    if not (name.startswith("block_") and name.endswith(".bin")):
                        continue
                    idx_str = name[len("block_"):-len(".bin")]
                    if not idx_str.isdigit():
                        continue
                    idx = int(idx_str)
                    if 0 <= idx < n:
                        bs.set(idx)
        except FileNotFoundError:
            pass
        self._present = bs

    def get_block_path(self, block_index: int) -> str:
        return os.path.join(self._blocks_dir, f"block_{block_index:0>8}.bin")

    # --- chunks.crc --------------------------------------------------------

    def _resize_crc_file(self):
        """Ensure chunks.crc is exactly chunk_number * 4 bytes (zero-filled if grown)."""
        if self.header is None:
            raise Exception("The header of cache file is None")
        need = self.header.chunk_number * 4
        if need == 0:
            try:
                os.remove(self._crc_path)
            except FileNotFoundError:
                pass
            return
        if not os.path.exists(self._crc_path):
            # Create an empty file so truncate can extend it.
            open(self._crc_path, "wb").close()
        with open(self._crc_path, "r+b") as f:
            f.truncate(need)
            f.flush()
            os.fsync(f.fileno())

    def _block_real_len(self, block_index: int) -> int:
        """Length of real (unpadded) payload bytes for ``block_index``."""
        bs = self._get_block_size()
        start = block_index * bs
        return max(0, min(bs, self._get_file_size() - start))

    def _block_chunk_count(self, block_index: int) -> int:
        cs = self._get_chunk_size()
        return (self._block_real_len(block_index) + cs - 1) // cs

    def _read_chunk_crcs(self, block_index: int) -> Optional[List[int]]:
        """Read the chunk CRC slots covering ``block_index`` (None if absent/too short)."""
        if self.header is None:
            return None
        cpb = self.header.chunks_per_block()
        count = self._block_chunk_count(block_index)
        start = block_index * cpb
        try:
            with open(self._crc_path, "rb") as f:
                f.seek(start * 4)
                data = f.read(count * 4)
        except FileNotFoundError:
            return None
        if len(data) < count * 4:
            return None
        return list(struct.unpack(f"<{count}I", data))

    def _write_chunk_crcs(self, block_index: int, crcs: List[int]) -> None:
        if self.header is None:
            raise Exception("The header of cache file is None")
        cpb = self.header.chunks_per_block()
        start = block_index * cpb
        data = struct.pack(f"<{len(crcs)}I", *crcs)
        with open(self._crc_path, "r+b") as f:
            f.seek(start * 4)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

    def _iter_chunk_spans(self, block_index: int, real_len: int):
        """Yield (local_offset, length) for each chunk within a block's real payload."""
        cs = self._get_chunk_size()
        off = 0
        while off < real_len:
            yield off, min(cs, real_len - off)
            off += cs

    # --- write -------------------------------------------------------------

    async def write_block(self, block_index: int, block_bytes: bytes) -> None:
        if not self.is_open:
            raise Exception("This file has been closed.")
        if self.path is None:
            raise Exception("The path of the cache file is None.")
        if block_index >= self._get_block_number():
            raise Exception("Invalid block index.")
        if self.header is None:
            raise Exception("The header of cache file is None.")
        if len(block_bytes) != self._get_block_size():
            raise Exception("Block size does not match the cache's block size.")

        algo = self.header.compression_algo
        real_len = self._block_real_len(block_index)
        real_payload = block_bytes[:real_len]

        # Per-chunk CRCs over the *logical* payload (independent of algo).
        chunk_crcs: List[int] = [
            zlib.crc32(real_payload[off:off + length]) & 0xFFFFFFFF
            for off, length in self._iter_chunk_spans(block_index, real_len)
        ]

        block_path = self.get_block_path(block_index)
        tmp_path = os.path.join(
            self._blocks_dir, f".block_{block_index:0>8}_{os.getpid()}.tmp"
        )

        def encode() -> bytes:
            if algo == 0:
                return real_payload
            sub_lens: List[int] = []
            blobs: List[bytes] = []
            for off, length in self._iter_chunk_spans(block_index, real_len):
                comp = _compress_chunk(real_payload[off:off + length], algo)
                sub_lens.append(len(comp))
                blobs.append(comp)
            out = bytearray()
            out += b"OLBK"
            out += struct.pack("<I", len(sub_lens))
            if sub_lens:
                out += struct.pack(f"<{len(sub_lens)}I", *sub_lens)
            out += struct.pack("<B", 0)  # filter_id (reserved)
            for b in blobs:
                out += b
            return bytes(out)

        def do_write():
            encoded = encode()
            with open(tmp_path, "wb") as f:
                f.write(encoded)
                f.flush()
                os.fsync(f.fileno())
            # Publish chunk CRCs BEFORE the block becomes visible (has_block),
            # so any reader that sees the block also sees its CRCs.
            self._write_chunk_crcs(block_index, chunk_crcs)
            os.replace(tmp_path, block_path)
            self._fsync_dir(self._blocks_dir)

        try:
            await fastapi.concurrency.run_in_threadpool(do_write)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
        # Block presence is tracked by file existence; mirror it in the bitmap so
        # bulk iterators (is_fully_cached / get_contiguous_ranges) stay in sync.
        with self._header_lock:
            if self._present is not None:
                try:
                    self._present.set(block_index)
                except IndexError:
                    pass

    @staticmethod
    def _fsync_dir(dir_path: str) -> None:
        """Best-effort fsync of the directory so the rename is durable."""
        try:
            fd = os.open(dir_path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    # --- read --------------------------------------------------------------

    async def read_block(self, block_index: int) -> Optional[bytes]:
        """Return the whole (block_size-padded) block payload, verifying chunk CRCs."""
        if not self.is_open:
            raise Exception("This file has been closed.")
        if self.path is None:
            raise Exception("The path of the cache file is None.")
        if block_index >= self._get_block_number():
            raise Exception("Invalid block index.")
        if self.header is None:
            raise Exception("The header of cache file is None.")
        if not self.has_block(block_index):
            return None

        algo = self.header.compression_algo
        block_path = self.get_block_path(block_index)
        real_len = self._block_real_len(block_index)
        bs = self._get_block_size()

        def read_and_verify() -> bytes:
            with portalocker.Lock(block_path, "rb", flags=portalocker.LOCK_SH) as fh:
                if algo == 0:
                    payload = fh.read(real_len)
                else:
                    payload = self._decompress_block(fh, algo, real_len)
            self._verify_block(block_index, payload, real_len)
            if len(payload) < bs:
                payload = payload + b"\x00" * (bs - len(payload))
            return payload

        try:
            return await fastapi.concurrency.run_in_threadpool(read_and_verify)
        except FileNotFoundError:
            # Block was evicted between has_block() and the open. Treat as a miss
            # (clear the stale bitmap bit) so the caller re-fetches instead of
            # surfacing an opaque 500.
            with self._header_lock:
                if self._present is not None:
                    try:
                        self._present.clear(block_index)
                    except IndexError:
                        pass
            return None

    def _decompress_block(self, fh: BinaryIO, algo: int, real_len: int) -> bytes:
        raw = fh.read()
        if len(raw) < 9 or raw[:4] != b"OLBK":
            raise CacheIntegrityError("Compressed block missing OLBK header.")
        (sub_count,) = struct.unpack("<I", raw[4:8])
        off = 8
        need = off + sub_count * 4 + 1
        if len(raw) < need:
            raise CacheIntegrityError("Compressed block index truncated.")
        sub_lens = struct.unpack(f"<{sub_count}I", raw[off:off + sub_count * 4])
        off += sub_count * 4 + 1  # +1 for filter_id
        out = bytearray()
        for slen in sub_lens:
            out += _decompress_chunk(raw[off:off + slen], algo)
            off += slen
        return bytes(out[:real_len])

    def _verify_block(self, block_index: int, payload: bytes, real_len: int) -> None:
        expected = self._read_chunk_crcs(block_index)
        if expected is None:
            raise CacheIntegrityError("Missing chunk CRCs for an existing block.")
        for i, (off, length) in enumerate(self._iter_chunk_spans(block_index, real_len)):
            got = zlib.crc32(payload[off:off + length]) & 0xFFFFFFFF
            if got != expected[i]:
                raise CacheIntegrityError(
                    f"Chunk CRC mismatch in block {block_index} chunk {i}."
                )

    async def _invalidate_block(self, block_index: int) -> None:
        """Drop a corrupt block (and zero its CRC slots) so it is re-fetched."""
        block_path = self.get_block_path(block_index)
        try:
            os.remove(block_path)
        except FileNotFoundError:
            pass
        if self.header is None:
            return
        cpb = self.header.chunks_per_block()
        count = self._block_chunk_count(block_index)
        try:
            with open(self._crc_path, "r+b") as f:
                f.seek(block_index * cpb * 4)
                f.write(b"\x00" * (count * 4))
                f.flush()
                os.fsync(f.fileno())
        except FileNotFoundError:
            pass
        with self._header_lock:
            if self._present is not None:
                try:
                    self._present.clear(block_index)
                except IndexError:
                    pass

    async def stream_range(self, start_pos: int, end_pos: int) -> AsyncIterator[bytes]:
        """Yield verified ~chunk-size pieces for [start_pos, end_pos).

        Dispatches by compression_algo. Uncompressed blocks are read directly at
        canonical offsets; compressed blocks decompress only the overlapping
        sub-chunks. Either path verifies each served chunk against chunks.crc and
        raises CacheIntegrityError on mismatch (the block is then invalidated).
        """
        if not self.is_open:
            raise Exception("This file has been closed.")
        if self.header is None:
            raise Exception("The header of cache file is None.")
        if start_pos < 0 or end_pos <= start_pos:
            return

        bs = self._get_block_size()
        start_block = start_pos // bs
        end_block = (end_pos - 1) // bs

        for cur_block in range(start_block, end_block + 1):
            if not self.has_block(cur_block):
                # Block vanished (e.g. whole-entry eviction) between the coverage
                # check and now. Drop it and surface a typed error the caller can
                # recover from (re-fetch from upstream) instead of an opaque 500.
                await self._invalidate_block(cur_block)
                raise CacheIntegrityError(f"Block {cur_block} is not available.")
            try:
                async for piece in self._stream_block_range(cur_block, start_pos, end_pos):
                    if piece:
                        yield piece
            except CacheIntegrityError:
                await self._invalidate_block(cur_block)
                raise
            except FileNotFoundError:
                # Block file removed mid-read (eviction). Same recovery path.
                await self._invalidate_block(cur_block)
                raise CacheIntegrityError(f"Block {cur_block} vanished mid-read.")

    async def _stream_block_range(
        self, block_index: int, range_start: int, range_end: int
    ) -> AsyncIterator[bytes]:
        bs = self._get_block_size()
        block_start = block_index * bs
        block_end = min((block_index + 1) * bs, self._get_file_size())
        need_start = max(range_start, block_start)
        need_end = min(range_end, block_end)

        # All blocking work for this block -- the shared block-file lock, the
        # chunks.crc read, and the chunk reads + CRC verification -- runs in ONE
        # threadpool hop so nothing blocks the event loop. Returns the verified
        # bytes for [need_start, need_end) or raises CacheIntegrityError.
        verified = await fastapi.concurrency.run_in_threadpool(
            self._read_verify_block_range, block_index, need_start, need_end
        )
        # Yield in ~chunk_size pieces (pure memory slicing, no IO) to keep network
        # writes bounded.
        cs = self._get_chunk_size()
        for off in range(0, len(verified), cs):
            piece = verified[off:off + cs]
            if piece:
                yield piece

    def _read_verify_block_range(
        self, block_index: int, need_start: int, need_end: int
    ) -> bytes:
        """Read + CRC-verify the slice [need_start, need_end) of one block.

        ``need_start``/``need_end`` are absolute file offsets already clamped to
        this block. Raises ``CacheIntegrityError`` on any short read or CRC
        mismatch. Synchronous by design -- called via ``run_in_threadpool``.
        """
        bs = self._get_block_size()
        algo = self._get_compression_algo()
        block_start = block_index * bs
        block_end = min((block_index + 1) * bs, self._get_file_size())
        real_len = block_end - block_start
        block_path = self.get_block_path(block_index)

        expected_crcs = self._read_chunk_crcs(block_index)
        if expected_crcs is None:
            raise CacheIntegrityError(f"Missing chunk CRCs for block {block_index}.")

        out = bytearray()
        with portalocker.Lock(block_path, "rb", flags=portalocker.LOCK_SH) as fh:
            sub_lens = data_base = None
            if algo != 0:
                sub_lens, data_base = self._read_compressed_index(fh)
            for chunk_idx, (coff, clen) in enumerate(self._iter_chunk_spans(block_index, real_len)):
                gstart = block_start + coff
                gend = gstart + clen
                if gend <= need_start or gstart >= need_end:
                    continue
                if algo == 0:
                    data = self._read_uncompressed(fh, coff, clen)
                else:
                    off = data_base + sum(sub_lens[:chunk_idx])
                    data = self._read_compressed_sub(fh, off, sub_lens[chunk_idx], algo)
                if len(data) != clen:
                    raise CacheIntegrityError(
                        f"Short read in block {block_index} chunk {chunk_idx}."
                    )
                if (zlib.crc32(data) & 0xFFFFFFFF) != expected_crcs[chunk_idx]:
                    raise CacheIntegrityError(
                        f"Chunk CRC mismatch in block {block_index} chunk {chunk_idx}."
                    )
                lo = max(need_start, gstart) - gstart
                hi = min(need_end, gend) - gstart
                piece = data[lo:hi]
                if piece:
                    out += piece
        return bytes(out)

    @staticmethod
    def _read_uncompressed(fh: BinaryIO, off: int, clen: int) -> bytes:
        fh.seek(off)
        return fh.read(clen)

    @staticmethod
    def _read_compressed_index(fh: BinaryIO):
        fh.seek(0)
        head = fh.read(8)
        if len(head) < 8 or head[:4] != b"OLBK":
            raise CacheIntegrityError("Compressed block missing OLBK header.")
        (sub_count,) = struct.unpack("<I", head[4:8])
        lens_bytes = fh.read(sub_count * 4 + 1)
        if len(lens_bytes) < sub_count * 4 + 1:
            raise CacheIntegrityError("Compressed block index truncated.")
        sub_lens = list(struct.unpack(f"<{sub_count}I", lens_bytes[:sub_count * 4]))
        data_base = 8 + sub_count * 4 + 1  # +1 for filter_id
        return sub_lens, data_base

    @staticmethod
    def _read_compressed_sub(fh: BinaryIO, off: int, slen: int, algo: int) -> bytes:
        fh.seek(off)
        return _decompress_chunk(fh.read(slen), algo)
