"""
Wrapper for zlib's Python class implementing the CompressionAlgorithm interface.
"""
from compression_interface import CompressionAlgorithm, CompressionStream, DecompressionStream

import zlib


class Zlib(CompressionAlgorithm):
    def __init__(self):
        pass

    def compress(self, data: bytes, level=-1):
        return zlib.compress(data, level)

    def compress_string(self, data_string, level=-1):
        return self.compress(data_string.encode(), level)
    
    def decompress(self, compressed_data):
        return zlib.decompress(compressed_data)
    
    def decompress_to_string(self, compressed_data: bytes):
        return self.decompress(compressed_data).decode()


class ZlibCompressionStream(CompressionStream):
    def __init__(self, level):
        self.compression_object = zlib.compressobj(level=level)
        self.compressed = b''
        self.finished = False


    def compress(self, data: bytes):
        # Returns the compression of the input bytes to this call.
        if self.finished: return
        c = self.compression_object.compress(data)
        self.compressed += c
        return c

    def finish(self):
        # Returns the entire compression of input bytes.
        if self.finished: return
        self.compressed += self.compression_object.flush()
        self.finished = True
        return self.compressed
    
    def __getitem__(self, val):
        return self.compressed[val]
    
    def decompress(self):
        if not self.finished: return
        return Zlib().decompress(self.compressed)


class ZlibDecompressionStream(DecompressionStream):
    def __init__(self):
        self.decompression_object = zlib.decompressobj()
        self.decompressed = b''
        self.finished = False

    def decompress(self, compressed_data: bytes):
        d = self.decompression_object.decompress(compressed_data)
        self.decompressed += d
        return d

    def finish(self):
        if self.finished: return
        self.decompressed += self.decompression_object.flush()
        self.finished = True
        return self.decompressed