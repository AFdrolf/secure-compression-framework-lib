from collections.abc import Callable
from pathlib import Path

from secure_compression_framework_lib.multi_stream.compress import (
    MSCompressor,
    MSDecompressor,
    ZlibCompressionStream,
    ZlibDecompressionStream,
)
from secure_compression_framework_lib.partitioner.access_control import Principal, SQLiteDataUnit
from secure_compression_framework_lib.partitioner.types.sqlite_advanced import SQLiteAdvancedPartitioner


def compress_sqlite_advanced(
    db_path: Path, access_control_policy: Callable[[SQLiteDataUnit], Principal], partition_policy: Callable[[Principal], str]
) -> bytes:
    """Implements end to end safe compression for SQLite

    The application provides a Python function that extracts the principal from an SQLiteDataUnit (table + row) for
    use as the access control policy

    Args:
    ----
        db_path: The SQLite DB to be compressed
        access_control_policy: Access control policy provided by application

    Returns:
    -------
        Bytes of the safely compressed database

    """
    partitioner = SQLiteAdvancedPartitioner(db_path, access_control_policy, partition_policy)
    bucketed_data = partitioner.partition()
    msc = MSCompressor(ZlibCompressionStream)
    for bucket, data in bucketed_data:
        msc.compress(bucket, data)
    return msc.finish()


def decompress_sqlite_advanced(ms_compressed_data: bytes) -> bytes:
    msd = MSDecompressor(ZlibDecompressionStream)
    msd.decompress(ms_compressed_data)
    return msd.finish()
