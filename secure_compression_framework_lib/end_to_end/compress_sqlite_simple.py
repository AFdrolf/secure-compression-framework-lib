from secure_compression_framework_lib.multi_stream.compress import MSCompressor, ZlibCompressionStream
from secure_compression_framework_lib.partitioner.types.sqlite_simple import SQLiteSimplePartitioner


def compress_sqlite_simple(data, access_control_policy, partition_policy):
    partitioner = SQLiteSimplePartitioner(data, access_control_policy, partition_policy)
    db_bucket_paths = partitioner.partition()

    msc = MSCompressor(ZlibCompressionStream)
    for db_bucket_path in db_bucket_paths:
        with db_bucket_path.open(mode="rb") as f:
            msc.compress(db_bucket_path.name, f.read())

    return msc.finish()