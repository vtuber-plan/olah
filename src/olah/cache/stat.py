# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Cache inspection tool for the v10 on-disk block cache.

Run directly to print a cache entry's header + per-block presence (derived from a
single ``blocks/`` scan, not a stat per block)::

    python -m olah.cache.stat --file <path-to-cache-entry-dir>
    python -m olah.cache.stat -f repos/files/.../resolve/main/blob.safetensors

Optionally export the reassembled file when every block is present::

    python -m olah.cache.stat -f <entry> --export out.bin
"""

import argparse
import os
import sys

from olah.cache.olah_cache import OlahCache


def _size_human(size: int) -> str:
    for unit, factor in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if size >= factor:
            return f"{size / factor:.4f}{unit}"
    return f"{size:.4f}B"


def _present_blocks(cache_dir: str, block_number: int):
    """Return a list of bools (length block_number) from one blocks/ scan.

    Falls back to ``has_block`` (per-block stat) only if the directory scan is
    unavailable, so this stays correct even outside an open ``OlahCache``.
    """
    present = [False] * max(block_number, 0)
    blocks_dir = os.path.join(cache_dir, "blocks")
    try:
        names = os.listdir(blocks_dir)
    except OSError:
        return present
    for name in names:
        if not (name.startswith("block_") and name.endswith(".bin")):
            continue
        idx_str = name[len("block_"):-len(".bin")]
        if not idx_str.isdigit():
            continue
        idx = int(idx_str)
        if 0 <= idx < block_number:
            present[idx] = True
    return present


def main() -> int:
    parser = argparse.ArgumentParser(description="Olah v10 cache visualization tool.")
    parser.add_argument("--file", "-f", type=str, required=True, help="Path of the Olah cache entry directory")
    parser.add_argument("--export", "-e", type=str, default="", help="Export the cached file if all blocks are present")
    args = parser.parse_args()

    cache_dir = args.file
    if not os.path.isdir(cache_dir):
        print(f"Not a cache entry directory: {cache_dir}", file=sys.stderr)
        return 1

    # On-disk size of the whole entry (meta.bin + chunks.crc + blocks/*).
    on_disk = 0
    for root, _dirs, files in os.walk(cache_dir):
        for name in files:
            try:
                on_disk += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass

    try:
        cache = OlahCache(cache_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to open cache: {exc}", file=sys.stderr)
        return 1

    try:
        h = cache.header
        block_number = h.block_number
        present = _present_blocks(cache_dir, block_number)
        cached = sum(present)

        print(f"File: {cache_dir}")
        print(f"Olah Cache Version: {h.version}")
        print(f"File Size: {_size_human(h.file_size)} ({h.file_size} B)")
        print(f"On-Disk Size: {_size_human(on_disk)} ({on_disk} B)")
        print(f"Block Size: {_size_human(h.block_size)}")
        print(f"Chunk Size: {_size_human(h.chunk_size)}")
        print(f"Compression: {h.compression_algo} (0=none,1=gzip,2=lzma)")
        print(f"Etag: {h.etag.decode('utf-8', 'replace') if h.etag else '(none)'}")
        print(f"Blocks: {cached}/{block_number} cached")
        print("Cache Status:")
        status = "".join("1" if p else "0" for p in present)
        for i in range(0, len(status), 50):
            print("  " + status[i:i + 50])

        if args.export:
            if block_number > 0 and cached == block_number:
                import asyncio

                async def _export():
                    with open(args.export, "wb") as out:
                        for idx in range(block_number):
                            block = await cache.read_block(idx)
                            if block is None:
                                raise RuntimeError(f"block {idx} vanished during export")
                            out.write(block[: cache._block_real_len(idx)])

                asyncio.run(_export())
                print(f"Exported {h.file_size} bytes to {args.export}")
            else:
                print("Some blocks are not cached, so the export is skipped.")
    finally:
        cache.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
