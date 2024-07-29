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
    # compress_obj = zlib.compressobj()

    # # Compress data in chunks
    # data1 = b'This is the first part of the data. '
    # data2 = b'This is the second part of the data.'

    # compressed_data1 = compress_obj.compress(data1)
    # compressed_data2 = compress_obj.compress(data2)

    # # Finalize the compression
    # compressed_data_final = compress_obj.flush()

    # # Combine all compressed data
    # compressed_data = compressed_data1 + compressed_data2 + compressed_data_final

    # # Print the compressed data
    # print("Compressed data:", compressed_data)
    # print(compressed_data1, compressed_data2,)

    # d = zlib.decompressobj()
    # print(d.decompress(compressed_data[:5])+d.decompress(compressed_data[5:]+d.flush()))

    # it = iter(range(10))
    # for i in it:
    #     if i == 7:
    #         next(it, None)
    #     else:
    #         print(i)

    from typing import Tuple
    import zlib


    def prepare(data: bytes) -> Tuple[int, bytes, int]:
        deflate = zlib.compressobj()
        result = deflate.compress(data)
        result += deflate.flush(zlib.Z_SYNC_FLUSH)
        return len(data), result, zlib.adler32(data)


    def concatenate(*chunks: Tuple[int, bytes, int]) -> bytes:
        if not chunks:
            return b''
        _, result, final_checksum = chunks[0]
        for length, chunk, checksum in chunks[1:]:
            result += chunk[2:]  # strip the zlib header
            final_checksum = adler32_combine(final_checksum, checksum, length)
        result += b'\x03\x00'  # insert a final empty block
        result += final_checksum.to_bytes(4, byteorder='big')
        return result


    def adler32_combine(adler1: int, adler2: int, length2: int) -> int:
        # Python implementation of adler32_combine
        # The orignal C implementation is Copyright (C) 1995-2011, 2016 Mark Adler
        # see https://github.com/madler/zlib/blob/master/adler32.c#L143
        BASE = 65521
        WORD = 0xffff
        DWORD = 0xffffffff
        if adler1 < 0 or adler1 > DWORD:
            raise ValueError('adler1 must be between 0 and 2^32')
        if adler2 < 0 or adler2 > DWORD:
            raise ValueError('adler2 must be between 0 and 2^32')
        if length2 < 0:
            raise ValueError('length2 must not be negative')

        remainder = length2 % BASE
        sum1 = adler1 & WORD
        sum2 = (remainder * sum1) % BASE
        sum1 += (adler2 & WORD) + BASE - 1
        sum2 += ((adler1 >> 16) & WORD) + ((adler2 >> 16) & WORD) + BASE - remainder
        if sum1 >= BASE:
            sum1 -= BASE
        if sum1 >= BASE:
            sum1 -= BASE
        if sum2 >= (BASE << 1):
            sum2 -= (BASE << 1)
        if sum2 >= BASE:
            sum2 -= BASE

        return (sum1 | (sum2 << 16))
    
    hello = prepare(b'Hello World! ')
    test = prepare(b'This is a test. ')
    fox = prepare(b'The quick brown fox jumped over the lazy dog. ')
    dawn = prepare(b'We ride at dawn! ')

    # these all print what you would expect
    print(zlib.decompress(concatenate(hello, test, fox, dawn)))
    print(zlib.decompress(concatenate(dawn, fox, test, hello)))
    print(zlib.decompress(concatenate(fox, hello, dawn, test)))
    print(zlib.decompress(concatenate(test, dawn, hello, fox)))


    # Sample compressed data (replace this with your actual compressed data)
    compressed_data = b'\x78\x9c\x4b\x4c\x4e\x55\x28\xcf\x2f\xca\x4f\x4f\x55\x48\xad\x4c\x4a\x4d\x2f\x55\x54\x48\xcc\xcf\x4b\x2c\x4a\x2e\x55\x4e\x4f\x2d\x49\x2c\x54\x2e\xcc\x2d\x51\x50\x2e\x4a\x2e\x54\x2e\x01\x00\x9d\x5d\x0b\x64'

    # Create a decompress object
    decompressor = zlib.decompressobj()

    # Variable to store decompressed data
    decompressed_data = b''

    # Process the compressed data one byte at a time
    for byte in compressed_data:
        # Feed one byte at a time into the decompressobj
        decompressed_data += decompressor.decompress(bytes([byte]))

    # Finalize the decompression to get any remaining data
    decompressed_data += decompressor.flush()

    # Print or use the decompressed data
    print(decompressed_data.decode('utf-8'))
