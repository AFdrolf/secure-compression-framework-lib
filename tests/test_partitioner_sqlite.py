from pathlib import Path

from secure_compression_framework_lib.partitioner.access_control import Principal, generate_attribute_based_partition_policy
from secure_compression_framework_lib.partitioner.types.sqlite_advanced import SQLiteAdvancedPartitioner
from secure_compression_framework_lib.partitioner.types.sqlite_simple import SQLiteDataUnit, SQLiteSimplePartitioner
from tests.example_data.generate_test_db_sqlite import generate_test_db_sqlite


def gid_as_principal_access_control_policy(sqlite_du: SQLiteDataUnit):
    """Example access control policy function.

        Assumes that principals can have views on messages and the principal with a view on the message is encoded
        as the 'gid' field of each row.
    """
    if sqlite_du.table_name == "message":
        principal_gid = sqlite_du.row[1]
        return Principal(gid=principal_gid)
    else:
        return Principal(null=True)


def test_partitioner_sqlite_simple_gid_col_as_principal_test_db(tmpdir):
    test_db_sqlite = Path(generate_test_db_sqlite(tmpdir))
    partitioner = SQLiteSimplePartitioner(
        test_db_sqlite, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid")
    )
    out = partitioner.partition()
    assert len(out) == 4
    pass


def test_partitioner_sqlite_advanced_gid_col_as_principal_test_db(tmpdir):
    test_db_sqlite = Path(generate_test_db_sqlite(tmpdir))
    partitioner = SQLiteAdvancedPartitioner(
        test_db_sqlite, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid")
    )
    out = partitioner.partition()
    np_str = str(Principal(null=True))
    assert [x[0] for x in out] == [np_str, np_str, "7", "7", "7", "2", "1", "1", np_str, np_str]
    assert sum([len(x[1]) for x in out]) == test_db_sqlite.stat().st_size
