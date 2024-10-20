from pathlib import Path
from dataclasses import dataclass
import sqlite3
import sys

sys.path.append(sys.path[0] + "/../../..")
from secure_compression_framework_lib.partitioner.partitioner import Partitioner

# TODO: we may need to make this code more efficient to deal with large databases.


@dataclass
class SQLiteDataUnit:
    """An SQLiteDataUnit is the unit which is mapped to a Principal.

    The unit we actually want to map to a Principal is a cell in the database, but to do this mapping we need some context for
    the cell (e.g., what table it belongs to)
    """

    row: tuple
    table_name: str

class SQLiteSimplePartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for the SQLite database file to be partitioned."""

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> list[Path]:
        """Creates a new SQLite database (serialized using SQLite's database file format) for each partition. Ouputs the list of paths for the new database files."""
        db_buckets = {}
        db_bucket_paths = []

        con = sqlite3.connect(self.data)
        cur = con.cursor()

        # Save schema to later create identical `bucket' DBs
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table'")
        schema = cur.fetchall()

        # First, iterate through all rows of all tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        for table in tables:
            table_name = table[0]
            cur.execute(f"SELECT * FROM {table_name};")
            for row in cur:
                data_unit = SQLiteDataUnit(row, table_name)
                principal = self.access_control_policy(data_unit)
                if principal == None:
                    continue
                db_bucket_id = self.partition_policy(data_unit)

                # Create empty SQLite file if it does not exist yet
                if db_bucket_id not in db_buckets:
                    db_bucket_path = str(db_bucket_id) + self.data
                    db_bucket_paths.append(Path(db_bucket_path))
                    db_bucket_con = sqlite3.connect(db_bucket_path)
                    db_bucket_cur = db_bucket_con.cursor()
                    for table_schema in schema:
                        # sqlite_sequence table gets created automatically; error is thrown if created manually
                        if "sqlite_sequence" not in table_schema[0]:
                            db_bucket_cur.execute(table_schema[0])
                    db_buckets[db_bucket_id] = (db_bucket_con, db_bucket_cur)
                else:
                    db_bucket_cur = db_buckets[db_bucket_id][0]

                # Then, add row to its respective bucket DB
                db_bucket_cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join('?' * len(row))});", row)

        # Close connections to DBs
        for db_bucket_con, _ in db_buckets.values():
            db_bucket_con.commit()
            db_bucket_con.close()
        con.close()

        return db_bucket_paths


if __name__ == "__main__":
    partitioner = SQLiteSimplePartitioner(db_name)

    def access(table, row):
        if table[0] == "messages":
            return row[1]
        else:
            return "metadata"

    def partition_policy(id):
        return id

    partitioner.partition(partition_policy, access)
