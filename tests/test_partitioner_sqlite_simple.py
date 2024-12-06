from pathlib import Path

from secure_compression_framework_lib.partitioner.access_control import Principal, basic_partition_policy
from secure_compression_framework_lib.partitioner.types.sqlite_simple import SQLiteDataUnit, SQLiteSimplePartitioner
from tests.example_data.generate_test_db_sqlite import generate_test_db_sqlite


def example_extract_principal_from_sqlite(sqlite_du: SQLiteDataUnit):
    """Example access control policy function.

    Assumes that principals can have views on messages and the principal with a view on the message is encoded
    as the 'gid' field of each row.
    """
    if sqlite_du.table_name == "messages":
        principal_gid = sqlite_du.row[1]
        return Principal(gid=principal_gid)
    else:
        return Principal(gid="metadata")


def example_sender_partition_policy(principal: Principal):
    return principal.gid


def test_partitioner_sqlite_simple(tmpdir):
    test_db_sqlite = Path(generate_test_db_sqlite(tmpdir))
    partitioner = SQLiteSimplePartitioner(
        test_db_sqlite, example_extract_principal_from_sqlite, example_sender_partition_policy
    )
    out = partitioner.partition()
    assert len(out) == 4
    pass
