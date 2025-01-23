import sqlite3
from pathlib import Path

from injection_attacks_mitigation_framework.partitioner.access_control import SQLiteDataUnit
from injection_attacks_mitigation_framework.partitioner.partitioner import Partitioner


class SQLiteSimplePartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for the SQLite database file to be partitioned."""

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> list[Path]:
        """
        Creates a new SQLite database (serialized using SQLite's database file format) for each partition.
        Outputs the list of paths for the new database files.
        """
        db_buckets = {}
        db_bucket_paths = []

        con = sqlite3.connect(self._get_data())
        cur = con.cursor()

        # Save schema to later create identical 'bucket' DBs
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table'")
        schema = cur.fetchall()

        # Save indexes
        cur.execute("SELECT sql FROM sqlite_master WHERE type='index'")
        indexes = cur.fetchall()

        # First, iterate through all rows of all tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        for table in tables:
            table_name = table[0]
            cur.execute(f"SELECT * FROM {table_name};")
            for row in cur:
                data_unit = SQLiteDataUnit(row, table_name)
                principal = self.access_control_policy(data_unit)
                if principal is None:
                    continue
                db_bucket_id = self.partition_policy(principal)

                # Create empty SQLite file if it does not exist yet
                if db_bucket_id not in db_buckets:
                    db_bucket_path = self._get_data().parent / (str(db_bucket_id) + "_" + self._get_data().name)
                    db_bucket_paths.append(db_bucket_path)
                    db_bucket_con = sqlite3.connect(db_bucket_path)
                    db_bucket_cur = db_bucket_con.cursor()
                    for table_schema in schema:
                        # sqlite_sequence table gets created automatically; error is thrown if created manually
                        if (
                            "sqlite_sequence" not in table_schema[0]
                            and "message_ftsv2_content" not in table_schema[0]
                            and "message_ftsv2_segments" not in table_schema[0]
                            and "message_ftsv2_segdir" not in table_schema[0]
                            and "message_ftsv2_docsize" not in table_schema[0]
                            and "message_ftsv2_stat" not in table_schema[0]
                            and "labeled_messages_fts_content" not in table_schema[0]
                            and "labeled_messages_fts_segments" not in table_schema[0]
                            and "labeled_messages_fts_segdir" not in table_schema[0]
                            and "labeled_messages_fts_docsize" not in table_schema[0]
                            and "labeled_messages_fts_stat" not in table_schema[0]
                        ):
                            db_bucket_cur.execute(table_schema[0])
                    for index in indexes:
                        if index[0]:
                            db_bucket_cur.execute(index[0])
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
