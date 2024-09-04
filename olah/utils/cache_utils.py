# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.


import json
from typing import Dict, Mapping, Union


async def write_cache_request(
    save_path: str,
    status_code: int,
    headers: Union[Dict[str, str], Mapping],
    content: bytes,
) -> None:
    """
    Write the request's status code, headers, and content to a cache file.

    Args:
        head_path (str): The path to the cache file.
        status_code (int): The status code of the request.
        headers (Dict[str, str]): The dictionary of response headers.
        content (bytes): The content of the request.

    Returns:
        None
    """
    if not isinstance(headers, dict):
        headers = {k.lower(): v for k, v in headers.items()}
    rq = {
        "status_code": status_code,
        "headers": headers,
        "content": content.hex(),
    }
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(rq, ensure_ascii=False))


async def read_cache_request(save_path: str) -> Dict[str, str]:
    """
    Read the request's status code, headers, and content from a cache file.

    Args:
        save_path (str): The path to the cache file.

    Returns:
        Dict[str, str]: A dictionary containing the status code, headers, and content of the request.
    """
    with open(save_path, "r", encoding="utf-8") as f:
        rq = json.loads(f.read())

    rq["content"] = bytes.fromhex(rq["content"])
    return rq
