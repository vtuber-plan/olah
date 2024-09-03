# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import datetime
import os

import time
from typing import List, Optional, Tuple


def get_folder_size(folder_path: str) -> int:
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def sort_files_by_access_time(folder_path: str) -> List[Tuple[str, datetime.datetime]]:
    files = []

    # Get all file paths and time
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            file_path = os.path.join(dirpath, f)
            if not os.path.isfile(file_path):
                continue
            access_time = datetime.datetime.fromtimestamp(os.path.getatime(file_path))
            files.append((file_path, access_time))
    
    # Sort by accesstime
    sorted_files = sorted(files, key=lambda x: x[1])
    
    return sorted_files

def sort_files_by_modify_time(folder_path: str) -> List[Tuple[str, datetime.datetime]]:
    files = []

    # Get all file paths and time
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            file_path = os.path.join(dirpath, f)
            if not os.path.isfile(file_path):
                continue
            access_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            files.append((file_path, access_time))
    
    # Sort by modify time
    sorted_files = sorted(files, key=lambda x: x[1])
    
    return sorted_files

def sort_files_by_size(folder_path: str) -> List[Tuple[str, int]]:
    files = []

    # Get all file paths and sizes
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            file_path = os.path.join(dirpath, f)
            if not os.path.isfile(file_path):
                continue
            file_size = os.path.getsize(file_path)
            files.append((file_path, file_size))
    
    # Sort by file size
    sorted_files = sorted(files, key=lambda x: x[1])
    
    return sorted_files

def touch_file_access_time(filename: str):
    if not os.path.exists(filename):
        return
    now = time.time()
    stat_info = os.stat(filename)
    atime = stat_info.st_atime
    mtime = stat_info.st_mtime
    
    os.utime(filename, times=(now, mtime))

def convert_to_bytes(size_str) -> Optional[int]:
    size_str = size_str.strip().upper()
    multipliers = {
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }

    for unit in multipliers:
        if size_str.endswith(unit):
            size = int(size_str[: -len(unit)])
            return size * multipliers[unit]

    # Default use bytes
    try:
        return int(size_str)
    except ValueError:
        return None


def convert_bytes_to_human_readable(bytes: int) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while bytes >= 1024 and index < len(suffixes) - 1:
        bytes /= 1024
        index += 1
    return f"{bytes:.2f} {suffixes[index]}"
