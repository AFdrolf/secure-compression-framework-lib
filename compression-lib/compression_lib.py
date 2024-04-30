import compression_interface
import zlib_compression


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

