from fastapi.responses import StreamingResponse


async def build_streaming_response(generator, include_status_code: bool) -> StreamingResponse:
    status_code = 200
    if include_status_code:
        status_code = await generator.__anext__()
    headers = await generator.__anext__()
    return StreamingResponse(generator, status_code=status_code, headers=headers)
