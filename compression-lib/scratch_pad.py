# import compression_lib
import zlib
if __name__ == "__main__":
    # string = b"abcderghijklmnopqrstuvwxyz"
    # chunk_size = 7
    # ix = 0
    # while True:
    #     next = string[chunk_size*ix:chunk_size*(ix+1)]
    #     if next == b'':
    #         break

    #     start_stream(ix)
    #     feed_bytes_to_stream(ix, next)
    #     ix += 1
    
    # finish_all_streams()
    # print(COMPRESSION_STREAMS[0].decompress())
    # compression_lib.start_compression_stream(0)
    # compression_lib.start_compression_stream(7)
    # print(compression_lib.get_all_compression_streams())

    # class A:
    #     def __init__(self):
    #         pass
    
    # a = A()
    # setattr(a, 'c', 'b')
    # print(getattr(a, 'c'))
    # setattr(a, 'f', [])
    # getattr(a, 'f').append(1)
    # print(a.f)

    # Create a compression object
    compress_obj = zlib.compressobj()

    # Compress data in chunks
    data1 = b'This is the first part of the data. '
    data2 = b'This is the second part of the data.'

    compressed_data1 = compress_obj.compress(data1)
    compressed_data2 = compress_obj.compress(data2)

    # Finalize the compression
    compressed_data_final = compress_obj.flush()

    # Combine all compressed data
    compressed_data = compressed_data1 + compressed_data2 + compressed_data_final

    # Print the compressed data
    print("Compressed data:", compressed_data)
    print(compressed_data1, compressed_data2,)

    d = zlib.decompressobj()
    print(d.decompress(compressed_data[:5])+d.decompress(compressed_data[5:]+d.flush()))

