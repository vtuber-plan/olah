import os

from olah.utils.disk_utils import (
    convert_bytes_to_human_readable,
    convert_to_bytes,
    get_folder_size,
    sort_files_by_access_time,
    sort_files_by_modify_time,
    sort_files_by_size,
    touch_file_access_time,
)
from olah.utils.file_utils import make_dirs


def test_make_dirs_creates_parent_directories_for_file_paths(tmp_path):
    target_file = tmp_path / "nested" / "path" / "file.txt"

    make_dirs(str(target_file))

    assert target_file.parent.is_dir()


def test_get_folder_size_and_sort_helpers_cover_nested_files(tmp_path):
    small = tmp_path / "small.txt"
    medium = tmp_path / "nested" / "medium.txt"
    large = tmp_path / "nested" / "deep" / "large.txt"
    make_dirs(str(medium))
    make_dirs(str(large))

    small.write_bytes(b"a")
    medium.write_bytes(b"bb")
    large.write_bytes(b"ccc")

    os.utime(small, (10, 10))
    os.utime(medium, (20, 20))
    os.utime(large, (30, 30))

    assert get_folder_size(str(tmp_path)) == 6
    assert [path for path, _ in sort_files_by_size(str(tmp_path))] == [
        str(small),
        str(medium),
        str(large),
    ]
    assert [path for path, _ in sort_files_by_access_time(str(tmp_path))] == [
        str(small),
        str(medium),
        str(large),
    ]
    assert [path for path, _ in sort_files_by_modify_time(str(tmp_path))] == [
        str(small),
        str(medium),
        str(large),
    ]


def test_touch_file_access_time_updates_only_atime(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    os.utime(target, (100, 50))

    before = os.stat(target)
    touch_file_access_time(str(target))
    after = os.stat(target)

    assert after.st_mtime == before.st_mtime
    assert after.st_atime >= before.st_atime


def test_convert_size_helpers_handle_supported_units_and_invalid_values():
    assert convert_to_bytes("128") == 128
    assert convert_to_bytes("2KB") == 2 * 1024
    assert convert_to_bytes("3 mb") == 3 * 1024**2
    assert convert_to_bytes("4T") == 4 * 1024**4
    assert convert_to_bytes("invalid") is None

    assert convert_bytes_to_human_readable(512) == "512.00 B"
    assert convert_bytes_to_human_readable(2048) == "2.00 KB"
