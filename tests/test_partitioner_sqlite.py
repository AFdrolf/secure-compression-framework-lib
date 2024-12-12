from pathlib import Path

from secure_compression_framework_lib.partitioner.access_control import Principal, basic_partition_policy
from secure_compression_framework_lib.partitioner.types.sqlite_advanced import SQLiteAdvancedPartitioner
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
        return Principal(null=True)


def example_sender_partition_policy(principal: Principal):
    if principal.null:
        return ""
    else:
        return principal.gid


def test_partitioner_sqlite_simple(tmpdir):
    test_db_sqlite = Path(generate_test_db_sqlite(tmpdir))
    partitioner = SQLiteSimplePartitioner(
        test_db_sqlite, example_extract_principal_from_sqlite, example_sender_partition_policy
    )
    out = partitioner.partition()
    assert len(out) == 4
    pass


def test_partitioner_sqlite_advanced(tmpdir):
    test_db_sqlite = Path(generate_test_db_sqlite(tmpdir))
    partitioner = SQLiteAdvancedPartitioner(
        test_db_sqlite, example_extract_principal_from_sqlite, example_sender_partition_policy
    )
    out = partitioner.partition()
    assert [x[0] for x in out] == ["", "", 7, 7, 7, 2, 1, 1, "", ""]
    assert sum([len(x[1]) for x in out]) == test_db_sqlite.stat().st_size
