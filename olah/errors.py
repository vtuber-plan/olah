# coding=utf-8
# Copyright 2024 XiaHan
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from fastapi import Response
from fastapi.responses import JSONResponse


def error_repo_not_found() -> JSONResponse:
    return JSONResponse(
        content={"error": "Repository not found"},
        headers={
            "x-error-code": "RepoNotFound",
            "x-error-message": "Repository not found",
        },
        status_code=401,
    )


def error_page_not_found() -> JSONResponse:
    return JSONResponse(
        content={"error": "Sorry, we can't find the page you are looking for."},
        headers={
            "x-error-code": "RepoNotFound",
            "x-error-message": "Sorry, we can't find the page you are looking for.",
        },
        status_code=404,
    )


def error_entry_not_found_branch(branch: str, path: str) -> Response:
    return Response(
        headers={
            "x-error-code": "EntryNotFound",
            "x-error-message": f'{path} does not exist on "{branch}"',
        },
        status_code=404,
    )


def error_entry_not_found() -> Response:
    return Response(
        headers={
            "x-error-code": "EntryNotFound",
            "x-error-message": "Entry not found",
        },
        status_code=404,
    )


def error_revision_not_found(revision: str) -> Response:
    return JSONResponse(
        content={"error": f"Invalid rev id: {revision}"},
        headers={
            "x-error-code": "RevisionNotFound",
            "x-error-message": f"Invalid rev id: {revision}",
        },
        status_code=404,
    )


# Olah Custom Messages
def error_proxy_timeout() -> Response:
    return Response(
        headers={
            "x-error-code": "ProxyTimeout",
            "x-error-message": "Proxy Timeout",
        },
        status_code=504,
    )


def error_proxy_invalid_data() -> Response:
    return Response(
        headers={
            "x-error-code": "ProxyInvalidData",
            "x-error-message": "Proxy Invalid Data",
        },
        status_code=504,
    )
