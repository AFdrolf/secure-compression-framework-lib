class MultiStreamCompressor:
    """
    Manages multiple compression streams, providing functionalities for opening/closing individual streams, feeding data to specific streams, etc.

    Args:
        - compression_streams_type (CompressionStream): the type of compression algorithm to use for each individual compression stream. Must be a class object of a CompressionStream class.
    """
    def __init__(self, compression_streams_type, delimiter=b"", *parameters):
        # TODO: add support for different compression levels in each stream.
        self.compression_streams_type = compression_streams_type
        self.parameters = parameters
        self.compression_streams = {}
        self.stream_switch = []
        # TODO: Add support for multithreading.
        self.delimiter = delimiter

    # TODO: add support for writing data to file as it is compressed.
    def compress(self, stream_key, data):
        # TODO: Add support for multithreading.
        if not stream_key in self.compression_streams:
            self.compression_streams[stream_key] = self.compression_streams_type(*self.parameters)
        
        self.stream_switch.append(stream_key)
        return self.compression_streams[stream_key].compress(data+self.delimiter)

    def finish(self):
        """
        Returns:
            The compressed strings from each stream concatenated together.
        """
        compressed_all = b""
        for compression_stream in self.compression_streams.values():
            compressed_all += compression_stream.finish()
            compressed_all += self.delimiter
        
        return compressed_all, self.stream_switch
    

class MultiStreamDecompressor:
    def __init__(self, decompression_streams_type, delimiter=b""):
        self.decompression_streams_type = decompression_streams_type
        self.decompression_streams = {}
        self.delimiter = delimiter
        self.stream_switch = None

    def decompress(self, compressed_data, stream_switch, parallel=False):
        # # TODO: Add support for multithreading. If parallel: Find boundary, and process each stream in parallel.
        # Normal: decompress, until unused_data is found, then start new decompressionstream object. Also try to just do this: https://stackoverflow.com/questions/58402524/python-zlib-how-to-decompress-many-objects
        if not parallel:
            self.stream_switch = stream_switch
            for stream_key in stream_switch:
                if stream_key not in self.decompression_streams: self.decompression_streams[stream_key] = self.decompression_streams_type()

            iterator = iter(range(0, len(compressed_data)))
            stream_key_iter = 0
            to_decompress = b""
            for i in iterator:
                compressed_chunk = compressed_data[i:i+len(self.delimiter)]
                if compressed_chunk != self.delimiter:
                    to_decompress += compressed_chunk[0:1]
                    
                else:

                    self.decompression_streams[list(self.decompression_streams.keys())[stream_key_iter]].decompress(to_decompress)
                    to_decompress = b""
                    stream_key_iter += 1
                    for _ in range(len(self.delimiter)-1):
                        next(iterator, None)
            return
    
    def finish(self):
        pointers_stream = {}
        for stream_key, decompression_stream in self.decompression_streams.items():
           pointers_stream[stream_key] = 0
           decompression_stream.finish()
        
        decompressed_ordered = b""
        for stream in self.stream_switch:
            decompressed = b""
            i = pointers_stream[stream]
            while True:
                compressed_chunk = self.decompression_streams[stream].decompressed[i:i+len(self.delimiter)]
                if compressed_chunk != self.delimiter:
                    decompressed += compressed_chunk[0:1]
                    i += 1
                else:
                    decompressed_ordered += decompressed
                    pointers_stream[stream] = i + len(self.delimiter)
                    break
        return decompressed_ordered
            

def partition_and_compress(partition_policy, data, principals, compression_streams_type, delimiter=b"", *args):
    """
    Args:
        - partition_policy (generator): yields pairs of the form (class of chunk, data chunk)
    """
    # TODO: different *args for partition_policy and MultiStreamCompressor; add support for writing data to output file.
    multi_stream_compressor = MultiStreamCompressor(compression_streams_type, delimiter, *args)

    # TODO: add support for writing data to file as it is compressed.
    for (stream_key, data_chunk) in partition_policy(data, principals):
        compressed_data = multi_stream_compressor.compress(stream_key, data_chunk)
    
    return multi_stream_compressor.finish()

def decompress_multi_stream(compressed_data, stream_switch, compression_streams_type, delimiter=b""):
    multi_stream_decompressor = MultiStreamDecompressor(compression_streams_type, delimiter)
    multi_stream_decompressor.decompress(compressed_data, stream_switch)
    return multi_stream_decompressor.finish()


def partition_and_compress_resources(data, principals, data_to_resources, policy_resources, compression_streams_type, delimiter=b"", *args):
    partition_policy_resources_init = lambda d, p : partition_policy_resources(d, p, data_to_resources, policy_resources, *args)
    return partition_and_compress(partition_policy_resources_init, data, principals, compression_streams_type, delimiter)

def partition_policy_resources(data, principals, data_to_resources, policy_resources, *args):
    """
    Generic resource-based partition policy. Iterates over 'data', casting it to resources (as per 'data_to_resources', which is a generator) and computing policy_resources over these.
    """
    for data_chunk, resource in data_to_resources(data, *args):
        yield policy_resources(resource, principals), data_chunk