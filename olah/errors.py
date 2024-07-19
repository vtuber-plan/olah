


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


def error_page_not_found() -> Response:
    return Response(
        headers={
            "x-error-code": "RepoNotFound",
            "x-error-message": "Sorry, we can't find the page you are looking for.",
        },
        status_code=404,
    )
