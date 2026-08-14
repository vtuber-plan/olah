# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import os
import threading


def make_dirs(path: str):
    if os.path.isdir(path):
        save_dir = path
    else:
        save_dir = os.path.dirname(path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)


def fsync_dir(dir_path: str) -> None:
    """Best-effort fsync of a directory so a rename within it is durable."""
    try:
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def atomic_write_bytes(path: str, data: bytes) -> None:
    """Write ``data`` to ``path`` atomically: unique tmp -> fsync -> os.replace.

    A concurrent reader or a crash mid-write never observes a truncated file:
    the destination appears either wholly old or wholly new.
    """
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    tmp_path = os.path.join(
        parent, f".{os.path.basename(path)}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    with open(tmp_path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)
    fsync_dir(parent)


def atomic_write_text(path: str, text: str, encoding: str = "utf-8") -> None:
    """Atomically write ``text`` (encoded utf-8 by default) to ``path``."""
    atomic_write_bytes(path, text.encode(encoding))
