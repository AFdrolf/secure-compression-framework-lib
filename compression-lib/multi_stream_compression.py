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
        # self.decompression_object = decompression_streams_type()
        self.compression_streams = {}
        self.delimiter = delimiter

    def decompress(self, compressed_data, parallel=False):
        # # TODO: Add support for multithreading. If parallel: Find boundary, and process each stream in parallel.
        # Normal: decompress, until unused_data is found, then start new decompressionstream object. Also try to just do this: https://stackoverflow.com/questions/58402524/python-zlib-how-to-decompress-many-objects
        if not parallel:
            decompressed_data = b""
            iterator = iter(range(0, len(compressed_data)))
            for i in iterator:
                compressed_chunk = compressed_data[i:i+len(self.delimiter)]
                if compressed_chunk != self.delimiter:
                    decompressed_data += self.decompression_object.decompress(compressed_chunk[0:1])
                    # print(self.decompression_object.decompressed)
                    
                else:
                    # print("HERE")
                    for _ in range(len(self.delimiter)):
                        next(iterator, None)
            return decompressed_data
    
    def finish(self):
        return self.decompression_object.finish()


def partition_and_compress(partition_policy, data, principals, compression_streams_type, delimiter=b"", *args):
    """
    Args:
        - partition_policy (generator):
    """
    # TODO: different *args for partition_policy and MultiStreamCompressor; add support for writing data to output file.
    multi_stream_compressor = MultiStreamCompressor(compression_streams_type, delimiter, *args)

    # TODO: add support for writing data to file as it is compressed.
    for (stream_key, data_chunk) in partition_policy(data, principals):
        compressed_data = multi_stream_compressor.compress(stream_key, data_chunk)
    
    return multi_stream_compressor.finish()

def partition_policy_resources(data, principals, data_to_resources, policy_resources, *args):
    """
    Generic resource-based partition policy. Iterates over 'data', casting it to resources (as per 'data_to_resources', which is a generator) and computing policy_resources over these.
    """
    for data_chunk, resource in data_to_resources(data, *args):
        yield policy_resources(resource, principals), data_chunk

def partition_and_compress_resources(data, principals, data_to_resources, policy_resources, compression_streams_type, *args):
    partition_policy_resources = lambda d, p : partition_policy_resources(d, p, data_to_resources, policy_resources, *args)
    return partition_and_compress(partition_policy_resources, data, principals, compression_streams_type)

def decompress_multi_stream(compressed_data, compression_streams_type, delimiter=b""):
    multi_stream_decompressor = MultiStreamDecompressor(compression_streams_type, delimiter)
    multi_stream_decompressor.decompress(compressed_data)
    return multi_stream_decompressor.finish()