# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import datetime
import os
import glob
from typing import Dict, Literal, Optional, Tuple, Union
import json
from urllib.parse import ParseResult, urlencode, urljoin, urlparse, parse_qs, urlunparse
import httpx
from olah.configs import OlahConfig
from olah.constants import WORKER_API_TIMEOUT


def get_url_tail(parsed_url: Union[str, ParseResult]) -> str:
    """
    Extracts the tail of a URL, including path, parameters, query, and fragment.

    Args:
        parsed_url (Union[str, ParseResult]): The parsed URL or a string URL.

    Returns:
        str: The tail of the URL, including path, parameters, query, and fragment.
    """
    if isinstance(parsed_url, str):
        parsed_url = urlparse(parsed_url)
    url_tail = parsed_url.path
    if len(parsed_url.params) != 0:
        url_tail += f";{parsed_url.params}"
    if len(parsed_url.query) != 0:
        url_tail += f"?{parsed_url.query}"
    if len(parsed_url.fragment) != 0:
        url_tail += f"#{parsed_url.fragment}"
    return url_tail


def parse_range_params(file_range: str, file_size: int) -> Tuple[int, int]:
    """
    Parses the range parameters for a file request.

    Args:
        file_range (str): The range parameter string, e.g., 'bytes=1887436800-'.
        file_size (int): The size of the file.

    Returns:
        Tuple[int, int]: A tuple of start and end positions for the file range.
    """
    if "/" in file_range:
        file_range, _file_size = file_range.split("/", maxsplit=1)
    else:
        file_range = file_range
    if file_range.startswith("bytes="):
        file_range = file_range[6:]
    start_pos, end_pos = file_range.split("-")
    if len(start_pos) != 0:
        start_pos = int(start_pos)
    else:
        start_pos = 0
    if len(end_pos) != 0:
        end_pos = int(end_pos)
    else:
        end_pos = file_size - 1
    return start_pos, end_pos


class RemoteInfo(object):
    def __init__(self, method: str, url: str, headers: Dict[str, str]) -> None:
        """
        Represents information about a remote request.

        Args:
            method (str): The HTTP method of the request.
            url (str): The URL of the request.
            headers (Dict[str, str]): The headers of the request.
        """
        self.method = method
        self.url = url
        self.headers = headers


def check_url_has_param_name(url: str, param_name: str) -> bool:
    """
    Checks if a URL contains a specific query parameter.

    Args:
        url (str): The URL to check.
        param_name (str): The name of the query parameter.

    Returns:
        bool: True if the URL contains the parameter, False otherwise.
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return param_name in query_params


def get_url_param_name(url: str, param_name: str) -> Optional[str]:
    """
    Retrieves the value of a specific query parameter from a URL.

    Args:
        url (str): The URL to retrieve the parameter from.
        param_name (str): The name of the query parameter.

    Returns:
        Optional[str]: The value of the query parameter if found, None otherwise.
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    original_location = query_params.get(param_name)
    if original_location:
        return original_location[0]
    else:
        return None


def add_query_param(url: str, param_name: str, param_value: str) -> str:
    """
    Adds a query parameter to a URL.

    Args:
        url (str): The URL to add the parameter to.
        param_name (str): The name of the query parameter.
        param_value (str): The value of the query parameter.

    Returns:
        str: The modified URL with the added query parameter.
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    query_params[param_name] = [param_value]

    new_query = urlencode(query_params, doseq=True)
    new_url = urlunparse(parsed_url._replace(query=new_query))

    return new_url


def remove_query_param(url: str, param_name: str) -> str:
    """
    Removes a query parameter from a URL.

    Args:
        url (str): The URL to remove the parameter from.
        param_name (str): The name of the query parameter.

    Returns:
        str: The modified URL with the parameter removed.
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    if param_name in query_params:
        del query_params[param_name]

    new_query = urlencode(query_params, doseq=True)
    new_url = urlunparse(parsed_url._replace(query=new_query))

    return new_url


def clean_path(path: str) -> str:
    while ".." in path:
        path = path.replace("..", "")
    path = path.replace("\\", "/")
    while "//" in path:
        path = path.replace("//", "/")
    return path