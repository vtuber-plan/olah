from fastapi.responses import StreamingResponse

from olah.proxy.result import ProxyResult


async def build_streaming_response(result: ProxyResult) -> StreamingResponse:
    return StreamingResponse(result.body, status_code=result.status_code, headers=result.headers)
