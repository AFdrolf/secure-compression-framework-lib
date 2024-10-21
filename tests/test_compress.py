"""Tests for multi stream compression."""

import os

import pytest

from secure_compression_framework_lib.multi_stream.compress import (
    MSCompressor,
    MSDecompressor,
    ZlibCompressionStream,
    ZlibDecompressionStream,
)

TEST1 = b"The quick brown fox jumped over the lazy dog"
TEST2 = b"The quick brown fox jumped over the lazy dog round 2"


def test_compress_correctness():
    """Test compression correctness."""
    msc = MSCompressor(ZlibCompressionStream)
    msd = MSDecompressor(ZlibDecompressionStream)

    msc.compress("label", TEST1)
    msc.compress("label", TEST2)

    c, ss = msc.finish()

    msd.decompress(c, ss)
    d = msd.finish()

    assert d == TEST1 + TEST2
    assert len(c) < len(TEST1 + TEST2)


def test_compress_diff_labels():
    """Test compression handles different labels and compresses in separate streams."""
    msc_diff = MSCompressor(ZlibCompressionStream)
    msd = MSDecompressor(ZlibDecompressionStream)
    msc_diff.compress("label", TEST1)
    msc_diff.compress("label2", TEST2)
    c_diff, ss_diff = msc_diff.finish()

    msc_same = MSCompressor(ZlibCompressionStream)
    msc_same.compress("newlabel", TEST1)
    msc_same.compress("newlabel", TEST2)
    c_same, ss_same = msc_same.finish()

    msd.decompress(c_diff, ss_diff)
    d = msd.finish()

    assert d == TEST1 + TEST2
    assert len(c_diff) > len(c_same)


@pytest.mark.parametrize(
    "rdata",
    [
        os.urandom(30),
        b"test\|testtesttest",
        b"\xdeb\x16:\xfe\xdd\xd7\xfd\x01\xa9t\xe5:BC\x9cCwy\x0e\x9f\x07T\xd0N\xf1\xa7\xda;H\xc2\xa7\xa8\xda\xb7\xba\xf6\xb5\xf8\xaf\x94?\x06\xb69\x97|3\xccK",
    ],
)
def test_compress_blocks(rdata):
    """Test compression handles multiple blocks with different labels."""
    msc = MSCompressor(ZlibCompressionStream)
    msd = MSDecompressor(ZlibDecompressionStream)

    def blocks_to_resources(data):
        """Yield data block if it is an even block number, or else yield None."""
        block_size = 5
        block_number = 0
        while block_number * block_size < len(data):
            i = block_number * block_size
            block = data[i : i + block_size]
            out = block if block_number % 2 == 0 else None
            yield out, block
            block_number += 1

    for stream_key, data_chunk in blocks_to_resources(rdata):
        msc.compress(stream_key, data_chunk)

    compressed_data, stream_switch = msc.finish()

    msd.decompress(compressed_data, stream_switch)
    decompressed_data = msd.finish()

    assert decompressed_data == rdata
