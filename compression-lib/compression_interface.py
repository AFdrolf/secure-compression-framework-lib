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
        return self.compress(data_string.encode(), parameters)

    def decompress(self, compressed_data: bytes):
        """Decompress the input bytes of data to bytes"""
        pass

    def decompress_to_string(self, compressed_data: bytes):
        """Decompress the input bytes of data to string."""
        return self.decompress(compressed_data).decode()

    def start_compression_stream(self, id=None):
        pass