import os

import multi_stream_compression
import zlib_compression


############################ 
# Setup
############################
def generate_data(data_type, *args):
    if data_type == "simple":
        return os.urandom(args[0])


############################ 
# Simple example
############################
def compress_in_blocks_mod(data, block_size, number_of_classes, compression_streams_type, delimiter=b""):
    """
    Partitions data into blocks of size block_size, and compresses together all block numbers of same residue mod 'number_of_classes'
    """
    
    def partition_policy_blocks_mod(data, _):
        block_number = 0
        while (block_number)*block_size < len(data):
            i = block_number * block_size
            yield (block_number % number_of_classes, data[i:i+block_size])
            block_number += 1
        return

    return multi_stream_compression.partition_and_compress(partition_policy_blocks_mod, data, None, compression_streams_type, delimiter)

def decompress_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter=b""):
    return multi_stream_compression.decompress_multi_stream(compressed_data, stream_switch, decompression_streams_type, delimiter)


if __name__ == "__main__":
    example_type = "simple"
    compression_streams_type = zlib_compression.ZlibCompressionStream
    decompression_streams_type = zlib_compression.ZlibDecompressionStream
    delimiter = b"||"

    print("- EXAMPLE TYPE: %s" % example_type.upper())
    if example_type == "simple":
        block_size = 5
        number_of_classes = 2
        data_length = 20
        data = generate_data(example_type, data_length)
        print("\tData: %s" % data)
        compressed_data, stream_switch = compress_in_blocks_mod(data, block_size, number_of_classes, compression_streams_type, delimiter)
        print("\tCompressed data: %s" % compressed_data)
        decompressed_data = decompress_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter)
        print("\tDecompressed data: %s" % decompressed_data)
        print("\n\tChanging second block...")
        data = data[:block_size] + os.urandom(block_size) + data[2*block_size:]
        print("\tData: %s" % data)
        compressed_data, stream_switch = compress_in_blocks_mod(data, block_size, number_of_classes, compression_streams_type, delimiter)
        print("\tCompressed data: %s" % compressed_data)
        decompressed_data = decompress_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter)
        print("\tDecompressed data: %s" % decompressed_data)