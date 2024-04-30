from zlib_compression import *

if __name__ == "__main__":
    a = b"rgqergnioqerjgqo8ugr89qerhgqjergopqu8ghqergqijreg"
    c = Zlib()
    c = c.compress_string("abcdef")

    d = ZlibDecompressionStream()
    d.feed_bytes_to_decompress(c[:20])
    d.feed_bytes_to_decompress(c[20:])
    print(d.finish())