import compression_lib
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
    compression_lib.start_compression_stream(0)
    compression_lib.start_compression_stream(7)
    print(compression_lib.get_all_compression_streams())