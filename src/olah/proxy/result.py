from dataclasses import dataclass
from typing import AsyncIterator, Mapping, Union


BodyChunk = Union[bytes, str]


@dataclass(frozen=True)
class ProxyResult:
    status_code: int
    headers: Mapping[str, str]
    body: AsyncIterator[BodyChunk]


async def single_chunk_body(chunk: BodyChunk) -> AsyncIterator[BodyChunk]:
    yield chunk
