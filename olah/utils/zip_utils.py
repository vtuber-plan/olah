

from typing import Optional
import zlib


def decompress_data(raw_data: bytes, content_encoding: Optional[str]):
    # If result is compressed
    if content_encoding is not None:
        final_data = raw_data
        algorithms = content_encoding.split(',')
        for algo in algorithms:
            algo = algo.strip().lower()
            if algo == "gzip":
                try:
                    final_data = zlib.decompress(raw_data, zlib.MAX_WBITS | 16)  # 解压缩
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