import compression_interface
import zlib_compression


class MultiStreamCompressor:
    """
    Manages multiple compression streams, providing functionalities for opening/closing individual streams, feeding data to specific streams, etc.

    Args:
        - compression_streams_type (CompressionStream): the type of compression algorithm to use for each individual compression stream. Must be a class object of a CompressionStream class.
    """
    def __init__(self, compression_streams_type, delimiter=b"{}", *parameters):
        self.compression_streams_type = compression_streams_type
        self.parameters = parameters
        self.compression_streams = {}
        self.delimiter = delimiter

    def compress(self, stream_key, data):
        # Add support for multithreading.
        if not stream_key in self.compression_streams:
            self.compression_streams[stream_key] = self.compression_streams_type(*self.parameters)
        
        self.compression_streams[stream_key].compress(data)

    def finish(self):
        """
        Returns:
            The compressed strings from each stream concatenated together.
        """
        compressed_all = b""
        for compression_stream in self.compression_streams.values():
            compressed_all += compression_stream.finish()
        
        return compressed_all
    

class MultiStreamDecompressor:
    def __init__(self, decompression_streams_type):
        self.compression_streams_type = decompression_streams_type
        self.decompressed = b""

    def decompress(self, compressed_data, parallel=False):
        # If paralle: Find boundary, and process each stream in parallel.
        # Normal: decompress, until unused_data is found, then start new decompressionstream object. Also try to just do this: https://stackoverflow.com/questions/58402524/python-zlib-how-to-decompress-many-objects

        pass








COMPRESSION_STREAMS = {}
DECOMPRESSION_STREAMS = {}


def start_compression_stream(stream_key, type="zlib"):
    if type == "zlib":
        COMPRESSION_STREAMS[stream_key] = zlib_compression.ZlibCompressionStream()
    return COMPRESSION_STREAMS[stream_key]

def feed_bytes_to_compression_stream(stream_key, data):
    return COMPRESSION_STREAMS[stream_key].feed_bytes_to_compress(data)

def finish_compression_stream(stream_key):
    return COMPRESSION_STREAMS[stream_key].finish()

def finish_all_compression_streams():
    for stream in COMPRESSION_STREAMS.values():
        stream.finish()
    return

def get_compression_stream(stream_key):
    return COMPRESSION_STREAMS[stream_key]

def get_all_compression_streams():
    return COMPRESSION_STREAMS


# Decompression
def start_decompression_stream(stream_key, type="zlib"):
    if type == "zlib":
        DECOMPRESSION_STREAMS[stream_key] = zlib_compression.ZlibDecompressionStream()
    return DECOMPRESSION_STREAMS[stream_key]

def feed_bytes_to_decompression_stream(stream_key, compressed_data):
    return DECOMPRESSION_STREAMS[stream_key].feed_bytes_to_decompress(compressed_data)

def finish_decompression_stream(stream_key):
    return DECOMPRESSION_STREAMS[stream_key].finish()

def finish_all_decompression_streams():
    for stream in DECOMPRESSION_STREAMS.values():
        stream.finish()
    return

def get_decompression_stream(stream_key):
    return DECOMPRESSION_STREAMS[stream_key]

def get_all_decompression_streams():
    return DECOMPRESSION_STREAMS

