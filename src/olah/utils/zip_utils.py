"""
Handlers for Content-Encoding.

See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
"""

import codecs
import io
import typing
from typing import List, Optional, Union
import zlib
import brotli
import httpx


class DecodingError(httpx.RequestError):
    """
    Decoding of the response failed, due to a malformed encoding.
    """


class ContentDecoder:
    def decode(self, data: bytes) -> bytes:
        raise NotImplementedError()  # pragma: no cover

    def flush(self) -> bytes:
        raise NotImplementedError()  # pragma: no cover


class IdentityDecoder(ContentDecoder):
    """
    Handle unencoded data.
    """

    def decode(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


class DeflateDecoder(ContentDecoder):
    """
    Handle 'deflate' decoding.

    See: https://stackoverflow.com/questions/1838699
    """

    def __init__(self) -> None:
        self.first_attempt = True
        self.decompressor = zlib.decompressobj()

    def decode(self, data: bytes) -> bytes:
        was_first_attempt = self.first_attempt
        self.first_attempt = False
        try:
            return self.decompressor.decompress(data)
        except zlib.error as exc:
            if was_first_attempt:
                self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                return self.decode(data)
            raise DecodingError(str(exc)) from exc

    def flush(self) -> bytes:
        try:
            return self.decompressor.flush()
        except zlib.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc


class GZipDecoder(ContentDecoder):
    """
    Handle 'gzip' decoding.

    See: https://stackoverflow.com/questions/1838699
    """

    def __init__(self) -> None:
        self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)

    def decode(self, data: bytes) -> bytes:
        try:
            return self.decompressor.decompress(data)
        except zlib.error as exc:
            raise DecodingError(str(exc)) from exc

    def flush(self) -> bytes:
        try:
            return self.decompressor.flush()
        except zlib.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc


class BrotliDecoder(ContentDecoder):
    """
    Handle 'brotli' decoding.

    Requires `pip install brotlipy`. See: https://brotlipy.readthedocs.io/
        or   `pip install brotli`. See https://github.com/google/brotli
    Supports both 'brotlipy' and 'Brotli' packages since they share an import
    name. The top branches are for 'brotlipy' and bottom branches for 'Brotli'
    """

    def __init__(self) -> None:
        if brotli is None:  # pragma: no cover
            raise ImportError(
                "Using 'BrotliDecoder', but neither of the 'brotlicffi' or 'brotli' "
                "packages have been installed. "
                "Make sure to install httpx using `pip install httpx[brotli]`."
            ) from None

        self.decompressor = brotli.Decompressor()
        self.seen_data = False
        self._decompress: typing.Callable[[bytes], bytes]
        if hasattr(self.decompressor, "decompress"):
            # The 'brotlicffi' package.
            self._decompress = self.decompressor.decompress  # pragma: no cover
        else:
            # The 'brotli' package.
            self._decompress = self.decompressor.process  # pragma: no cover

    def decode(self, data: bytes) -> bytes:
        if not data:
            return b""
        self.seen_data = True
        try:
            return self._decompress(data)
        except brotli.error as exc:
            raise DecodingError(str(exc)) from exc

    def flush(self) -> bytes:
        if not self.seen_data:
            return b""
        try:
            if hasattr(self.decompressor, "finish"):
                # Only available in the 'brotlicffi' package.

                # As the decompressor decompresses eagerly, this
                # will never actually emit any data. However, it will potentially throw
                # errors if a truncated or damaged data stream has been used.
                self.decompressor.finish()  # pragma: no cover
            return b""
        except brotli.error as exc:  # pragma: no cover
            raise DecodingError(str(exc)) from exc


class MultiDecoder(ContentDecoder):
    """
    Handle the case where multiple encodings have been applied.
    """

    def __init__(self, children: typing.Sequence[ContentDecoder]) -> None:
        """
        'children' should be a sequence of decoders in the order in which
        each was applied.
        """
        # Note that we reverse the order for decoding.
        self.children = list(reversed(children))

    def decode(self, data: bytes) -> bytes:
        for child in self.children:
            data = child.decode(data)
        return data

    def flush(self) -> bytes:
        data = b""
        for child in self.children:
            data = child.decode(data) + child.flush()
        return data


SUPPORTED_DECODERS = {
    "identity": IdentityDecoder,
    "gzip": GZipDecoder,
    "deflate": DeflateDecoder,
    "br": BrotliDecoder,
}


class Decompressor(object):
    def __init__(self, algorithms: Union[str, List[str]]) -> None:
        if isinstance(algorithms, str):
            self.algorithms = [algorithms]
        else:
            self.algorithms = algorithms

        self.decoders = []
        for algo in self.algorithms:
            algo = algo.strip().lower()
            if algo in SUPPORTED_DECODERS:
                self.decoders.append(SUPPORTED_DECODERS[algo]())
            else:
                print(f"Unsupported compression algorithm: {algo}")

        self.decoder = MultiDecoder(self.decoders)

    def decompress(self, raw_chunk: bytes) -> bytes:
        return self.decoder.decode(raw_chunk)


def decompress_data(raw_data: bytes, content_encoding: Optional[str]) -> bytes:
    # If result is compressed
    if content_encoding is not None:
        final_data = raw_data
        algorithms = content_encoding.split(",")
        for algo in algorithms:
            algo = algo.strip().lower()
            if algo == "gzip":
                try:
                    final_data = zlib.decompress(
                        raw_data, zlib.MAX_WBITS | 16
                    )  # 解压缩
                except Exception as e:
                    print(f"Error decompressing gzip data: {e}")
            elif algo == "compress":
                print(f"Unsupported decompression algorithm: {algo}")
            elif algo == "deflate":
                try:
                    final_data = zlib.decompress(raw_data)
                except Exception as e:
                    print(f"Error decompressing deflate data: {e}")
            elif algo == "br":
                try:
                    import brotli

                    final_data = brotli.decompress(raw_data)
                except Exception as e:
                    print(f"Error decompressing Brotli data: {e}")
            elif algo == "zstd":
                try:
                    import zstandard

                    final_data = zstandard.ZstdDecompressor().decompress(raw_data)
                except Exception as e:
                    print(f"Error decompressing Zstandard data: {e}")
            else:
                print(f"Unsupported compression algorithm: {algo}")
        return final_data
    else:
        return raw_data
