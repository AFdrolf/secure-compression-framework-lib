"""
Wrapper for zlib's Python class implementing the CompressionAlgorithm interface.
"""
from compression_interface import CompressionAlgorithm

import zlib

class Zlib(CompressionAlgorithm):
    def __init__(self):
        pass

    def compress(self, data: bytes, level):
        return zlib.compress(data, level)

    def compress_string(self, data_string, level):
        return super(Zlib, self).compress(data_string, level)

    def decompress(self, compressed_data: bytes):
        return zlib.decompress(compressed_data)

    def decompress_to_string(self, compressed_data: bytes):
        return super(Zlib, self).decompress(compressed_data)

    def start_compression_stream(self, id=None):
        pass