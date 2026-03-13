import gzip
import zlib

import brotli
import pytest

from olah.utils.zip_utils import (
    BrotliDecoder,
    DecodingError,
    Decompressor,
    DeflateDecoder,
    GZipDecoder,
    IdentityDecoder,
    MultiDecoder,
    decompress_data,
)


def test_identity_decoder_returns_data_unchanged():
    decoder = IdentityDecoder()

    assert decoder.decode(b"plain-data") == b"plain-data"
    assert decoder.flush() == b""


def test_gzip_decoder_decompresses_valid_payload():
    payload = b"hello gzip"
    encoded = gzip.compress(payload)
    decoder = GZipDecoder()

    assert decoder.decode(encoded) + decoder.flush() == payload


def test_deflate_decoder_supports_zlib_wrapped_and_raw_streams():
    payload = b"hello deflate"
    wrapped = zlib.compress(payload)
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    raw = compressor.compress(payload) + compressor.flush()

    wrapped_decoder = DeflateDecoder()
    raw_decoder = DeflateDecoder()

    assert wrapped_decoder.decode(wrapped) + wrapped_decoder.flush() == payload
    assert raw_decoder.decode(raw) + raw_decoder.flush() == payload


def test_gzip_and_brotli_decoders_raise_on_invalid_data():
    with pytest.raises(DecodingError):
        GZipDecoder().decode(b"not-gzip")

    with pytest.raises(DecodingError):
        BrotliDecoder().decode(b"not-brotli")


def test_multi_decoder_reverses_encoding_order():
    payload = b"multi-stage payload"
    gzip_encoded = gzip.compress(payload)
    combined = brotli.compress(gzip_encoded)

    decoder = MultiDecoder([GZipDecoder(), BrotliDecoder()])

    assert decoder.decode(combined) + decoder.flush() == payload


def test_decompressor_ignores_unknown_algorithms_and_handles_single_known_one(capsys):
    payload = b"abc123"
    encoded = gzip.compress(payload)

    decoder = Decompressor(["unknown", "gzip"])

    assert decoder.decompress(encoded) == payload
    assert "Unsupported compression algorithm: unknown" in capsys.readouterr().out


def test_decompress_data_supports_common_single_algorithms():
    payload = b"payload"
    gzip_encoded = gzip.compress(payload)
    deflate_encoded = zlib.compress(payload)
    brotli_encoded = brotli.compress(payload)

    assert decompress_data(gzip_encoded, "gzip") == payload
    assert decompress_data(deflate_encoded, "deflate") == payload
    assert decompress_data(brotli_encoded, "br") == payload
    assert decompress_data(payload, None) == payload
