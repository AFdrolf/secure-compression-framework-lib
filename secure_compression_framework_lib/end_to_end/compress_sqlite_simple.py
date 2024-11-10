from pathlib import Path
from typing import Callable

from secure_compression_framework_lib.multi_stream.compress import MSCompressor, ZlibCompressionStream
from secure_compression_framework_lib.partitioner.access_control import Principal
from secure_compression_framework_lib.partitioner.types.sqlite_simple import SQLiteDataUnit, SQLiteSimplePartitioner


def compress_sqlite_simple(
    db_path: Path,
    access_control_policy: Callable[[SQLiteDataUnit], Principal],
    partition_policy: Callable[[Principal], str],
) -> bytes:
    partitioner = SQLiteSimplePartitioner(db_path, access_control_policy, partition_policy)
    db_bucket_paths = partitioner.partition()

    msc = MSCompressor(ZlibCompressionStream, stream_switch_delimiter=b"[|\\")
    for db_bucket_path in db_bucket_paths:
        with db_bucket_path.open(mode="rb") as f:
            msc.compress(db_bucket_path.name, f.read())

    return msc.finish()
