"""
Interface for a generic compression algorithm.
To be implemented by specific algorithms such as zlib, gzip, lz4, etc.
"""

class CompressionAlgorithm:
    def __init__(self):
        pass

    def compress(self, data: bytes, *parameters):
        """Compress the input bytes of data."""
        pass

    def compress_string(self, data_string, *parameters):
        """Compress the input string of data."""
        pass

    def decompress(self, compressed_data: bytes):
        """Decompress the input bytes of data to bytes"""
        pass

    def decompress_to_string(self, compressed_data: bytes):
        """Decompress the input bytes of data to string."""
        pass


class CompressionStream:
    def __init__(self):
        pass

    def feed_bytes_to_compress(self, bytes):
        pass

    def finish(self):
        pass

    def decompress(self):
        pass


class DecompressionStream:
    def __init__(self):
        pass

    def feed_bytes_to_decompress(self, compressed_data: bytes):
        pass

    def finish(self):
        pass