import os

import multi_stream_compression
import zlib_compression


############################ 
# Setup
############################
def generate_data(data_type, *args):
    if data_type == "simple":
        return os.urandom(args[0])
    
    elif data_type == "simple_resources":
        return os.urandom(args[0])


############################ 
# Simple examples
############################
def compress_in_blocks_mod(data, block_size, number_of_classes, compression_streams_type, delimiter=b""):
    """
    Partitions data into blocks of size 'block_size', and compresses together all block numbers of same residue mod 'number_of_classes'.
    """
    
    def partition_policy_blocks_mod(data, _):
        block_number = 0
        while (block_number)*block_size < len(data):
            i = block_number * block_size
            yield (block_number % number_of_classes, data[i:i+block_size])
            block_number += 1
        return

    return multi_stream_compression.partition_and_compress(partition_policy_blocks_mod, data, None, compression_streams_type, delimiter)

def decompress_in_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter=b""):
    return multi_stream_compression.decompress_multi_stream(compressed_data, stream_switch, decompression_streams_type, delimiter)


############################ 
# Simple examples with resources
############################
def compress_simple_resources(data, block_size, mod, compression_streams_type, delimiter=b""):
    """Partitions data into blocks of size 'block_size', and interprets the even blocks as resources. The policy for each resource is int(block) % 'mod'. Non-resources are compressed together in a meta-stream."""
    
    def blocks_to_resources(data):
        """Yields data block if it is an even block number, or else yields None."""
        block_number = 0
        while (block_number)*block_size < len(data):
            i = block_number * block_size
            block = data[i:i+block_size]
            out = block if block_number % 2 == 0 else None
            print("\tBlock %s maps to resource %s" % (block, out))
            yield block, out
            block_number += 1
        return

    def mod_4_policy_resource(resource, _):
        out = None if not resource else int(resource.hex(), 16) % mod
        print("\t\tResource is in class %s" % out)
        return out
    
    return multi_stream_compression.partition_and_compress_resources(data, None, blocks_to_resources, mod_4_policy_resource, compression_streams_type, delimiter)


def decompress_simple_resources(compressed_data, stream_switch, decompression_streams_type, delimiter=b""):
    return multi_stream_compression.decompress_multi_stream(compressed_data, stream_switch, decompression_streams_type, delimiter)

if __name__ == "__main__":
    example_type = "simple_resources"
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
        decompressed_data = decompress_in_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter)
        print("\tDecompressed data: %s" % decompressed_data)
        while True:
            block_to_change = input("\n\tEnter block to change: ")
            print("\tChanging block...")
            if block_to_change == data_length // 5:
                data_changed = data[:-block_size] + os.urandom(block_size)
            else:
                data_changed = data[:block_size*int(block_to_change)] + os.urandom(block_size) + data[(int(block_to_change)+1)*block_size:]
            print("\tData: %s" % data_changed)
            compressed_data, stream_switch = compress_in_blocks_mod(data_changed, block_size, number_of_classes, compression_streams_type, delimiter)
            print("\tCompressed data: %s" % compressed_data)
            decompressed_data = decompress_in_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter)
            print("\tDecompressed data: %s" % decompressed_data)
    
    elif example_type == "simple_resources":
        data_length = 40
        block_size = 5
        mod = 3
        data = generate_data(example_type, data_length)
        print("\tData: %s" % data)
        compressed_data, stream_switch = compress_simple_resources(data, block_size, mod, compression_streams_type, delimiter)
        print("\tCompressed data: %s" % compressed_data)
        decompressed_data = decompress_in_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter)
        print("\tDecompressed data: %s" % decompressed_data)
        while True:
            block_to_change = input("\n\tEnter block to change: ")
            print("\tChanging block...")
            if block_to_change == data_length // 5:
                data_changed = data[:-block_size] + os.urandom(block_size)
            else:
                data_changed = data[:block_size*int(block_to_change)] + os.urandom(block_size) + data[(int(block_to_change)+1)*block_size:]
            print("\tData: %s" % data_changed)
            compressed_data, stream_switch = compress_simple_resources(data_changed, block_size, mod, compression_streams_type, delimiter)
            print("\tCompressed data: %s" % compressed_data)
            decompressed_data = decompress_in_blocks_mod(compressed_data, stream_switch, decompression_streams_type, delimiter)
            print("\tDecompressed data: %s" % decompressed_data)