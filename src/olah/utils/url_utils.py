# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import datetime
import os
import glob
from typing import Dict, List, Literal, Optional, Tuple, Union
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


def parse_content_range(content_range: str) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
    """
    Parses a Content-Range header string and extracts the unit, start position, end position, and resource size.

    Args:
        content_range (str): The Content-Range header string, e.g., "bytes 0-999/1000".

    Returns:
        Tuple[str, Optional[int], Optional[int], Optional[int]]: A tuple containing:
            - unit (str): The unit of the range, typically "bytes".
            - start_pos (Optional[int]): The starting position of the range. None if the range is "*".
            - end_pos (Optional[int]): The ending position of the range. None if the range is "*".
            - resource_size (Optional[int]): The total size of the resource. None if the size is unknown.

    Raises:
        Exception: If the range unit is invalid or the range format is incorrect.
    """
    if content_range.startswith("bytes "):
        unit = "bytes"
        content_range_part = content_range[len("bytes "):]
    else:
        raise Exception("Invalid range unit")

    
    if "/" in content_range_part:
        data_range, resource_size = content_range_part.split("/", maxsplit=1)
        resource_size = int(resource_size)
    else:
        data_range = content_range_part
        resource_size = None
    
    if "-" in data_range:
        start_pos, end_pos = data_range.split("-")
        start_pos, end_pos = int(start_pos), int(end_pos)
    elif "*" == data_range.strip():
        start_pos, end_pos = None, None
    else:
        raise Exception("Invalid range")
    return unit, start_pos, end_pos, resource_size


def parse_range_params(range_header: str) -> Tuple[str, List[Tuple[Optional[int], Optional[int]]], Optional[int]]:
    """
    Parses the HTTP Range request header and returns the unit and a list of ranges.

    Args:
        range_header (str): The HTTP Range request header string, e.g., "bytes=0-499" or "bytes=200-999, 2000-2499, 9500-".

    Returns:
        Tuple[str, List[Tuple[int, int]], Optional[int]]: A tuple containing the unit (e.g., "bytes") and a list of ranges.
            Each range is represented as a tuple of start and end positions. If the end position is not specified,
            it is set to None. For suffix-length ranges (e.g., "-500"), the start position is negative.

    Raises:
        ValueError: If the Range header is empty or has an invalid format.
    """
    if not range_header:
        raise ValueError("Range header cannot be empty")

    # Split the unit and range specifiers
    parts = range_header.split('=')
    if len(parts) != 2:
        raise ValueError("Invalid Range header format")

    unit = parts[0].strip()  # Get the unit, typically "bytes"
    range_specifiers = parts[1].strip()  # Get the range part
    
    if range_specifiers.startswith("-") and range_specifiers[1:].isdigit():
        return unit, [], int(range_specifiers[1:])

    # Parse multiple ranges
    range_list = []
    for range_spec in range_specifiers.split(','):
        range_spec = range_spec.strip()
        if '-' not in range_spec:
            raise ValueError("Invalid range specifier")

        start, end = range_spec.split('-')
        start = start.strip()
        end = end.strip()

        # Handle suffix-length ranges (e.g., "-500")
        if not start and end:
            range_list.append((None, int(end)))  # Negative start indicates suffix-length
            continue

        # Handle open-ended ranges (e.g., "500-")
        if not end and start:
            range_list.append((int(start), None))
            continue

        # Handle full ranges (e.g., "200-999")
        if start and end:
            range_list.append((int(start), int(end)))
            continue

        # If neither start nor end is provided, it's invalid
        raise ValueError("Invalid range specifier")

    return unit, range_list, None


def get_all_ranges(file_size: int, unit: str, ranges: List[Tuple[Optional[int], Optional[int]]], suffix: Optional[int]) -> List[Tuple[int, int]]:
    all_ranges: List[Tuple[int, int]] = []
    if suffix is not None:
        all_ranges.append((file_size - suffix, file_size))
    else:
        for r in ranges:
            r_start = r[0] if r[0] is not None else 0
            r_end = r[1] if r[1] is not None else file_size - 1
            start_pos = max(0, r_start)
            end_pos = min(file_size - 1, r_end)
            if end_pos < start_pos:
                continue
            all_ranges.append((start_pos, end_pos + 1))
    return all_ranges


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