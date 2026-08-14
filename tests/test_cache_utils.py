# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for the API-response request cache (cache_utils).

Covers: atomic write (tmp+rename, no leftover .tmp), the JSON+raw-sidecar split
that replaces the old hex-doubled inline format, and backward-compatible reads of
legacy inline-hex caches.
"""

import asyncio
import glob
import json
import os

from olah.utils.cache_utils import read_cache_request, write_cache_request


def test_write_splits_json_and_raw_body_sidecar(tmp_path):
    save = str(tmp_path / "api" / "meta_get.json")
    content = bytes(range(256)) * 4  # 1024 bytes incl. all byte values

    asyncio.run(write_cache_request(save, 206, {"ETag": '"abc"', "X": "y"}, content))

    # The body lives in a sibling .body sidecar verbatim (NOT hex-doubled in JSON).
    assert os.path.exists(save)
    assert os.path.exists(save + ".body")
    assert os.path.getsize(save + ".body") == len(content)
    with open(save + ".body", "rb") as f:
        assert f.read() == content
    # The JSON no longer carries the body inline.
    with open(save, "r", encoding="utf-8") as f:
        assert "content" not in json.load(f)


def test_read_roundtrips_json_and_raw_body(tmp_path):
    save = str(tmp_path / "x.json")
    content = b"\x00\x01\x02 hello \xff\xfe"
    asyncio.run(write_cache_request(save, 200, {"a": "b"}, content))

    rq = asyncio.run(read_cache_request(save))
    assert rq["status_code"] == 200
    assert rq["headers"] == {"a": "b"}
    assert rq["content"] == content


def test_read_falls_back_to_legacy_inline_hex(tmp_path):
    save = str(tmp_path / "legacy.json")
    content = b"legacy-body\x00\xff"
    with open(save, "w", encoding="utf-8") as f:
        f.write(json.dumps({"status_code": 200, "headers": {"h": "1"}, "content": content.hex()}))

    rq = asyncio.run(read_cache_request(save))
    assert rq["status_code"] == 200
    assert rq["headers"] == {"h": "1"}
    assert rq["content"] == content


def test_write_is_atomic_no_tmp_leftover_and_valid(tmp_path):
    """A completed write leaves no .tmp files and a reader sees a complete file."""
    save = str(tmp_path / "a.json")
    asyncio.run(write_cache_request(save, 200, {"k": "v"}, b"payload"))

    leftovers = [p for p in glob.glob(str(tmp_path / "*.tmp"))]
    assert leftovers == [], f"atomic write left tmp files: {leftovers}"

    # A concurrent/late reader always gets a well-formed, complete record.
    rq = asyncio.run(read_cache_request(save))
    assert rq["content"] == b"payload" and rq["status_code"] == 200


def test_overwrite_replaces_both_files_atomically(tmp_path):
    save = str(tmp_path / "a.json")
    asyncio.run(write_cache_request(save, 200, {"v": "1"}, b"one"))
    asyncio.run(write_cache_request(save, 201, {"v": "2"}, b"two"))

    rq = asyncio.run(read_cache_request(save))
    assert rq["status_code"] == 201
    assert rq["headers"] == {"v": "2"}
    assert rq["content"] == b"two"
