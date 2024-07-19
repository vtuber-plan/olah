


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