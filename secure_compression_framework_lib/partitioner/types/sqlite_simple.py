import sqlite3

from secure_compression_framework_lib.partitioner.partitioner import Partitioner


# TODO: we may need to make this code more efficient to deal with large databases.

class SQLiteSimplePartitioner(Partitioner):
    def __init__(self, db_path):
        self.db_path = db_path
    
    def partition(self, partition_policy, access_control_policy):
        db_buckets = dict()


        con = sqlite3.connect(self.db_path)
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
                # TODO: pre-process row to make it compatible with access control policy (maybe just get the primary key)?
                principal_id = access_control_policy(row)
                db_bucket_id = partition_policy(principal_id)
                
                # Create empty SQLite file if it does not exist yet
                if db_bucket_id not in db_buckets:
                    # TODO: generalize so that user can specify name
                    db_bucket_path = db_bucket_id + self.db_path
                    bucket_con = sqlite3.connect(db_bucket_path)
                    bucket_cur = bucket_con.cursor()
                    for table_schema in schema:
                        bucket_cur.execute(table_schema[0])
                    db_buckets.append(bucket_con, bucket_cur)
                else:
                    bucket_cur = db_buckets[db_bucket_id][0]

                # Then, add row to its respective bucket DB
                bucket_cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join('?' * len(row))});", row)

        # Close connections to DB
        for bucket_con, _ in db_buckets.values():
            bucket_con.commit()
            bucket_con.close()
        con.close()

        return





