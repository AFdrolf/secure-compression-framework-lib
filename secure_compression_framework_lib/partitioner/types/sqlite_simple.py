import sqlite3

import sys
sys.path.append(sys.path[0] + '/../../..')
from secure_compression_framework_lib.partitioner.partitioner import Partitioner

# TODO: we may need to make this code more efficient to deal with large databases.


class SQLiteSimplePartitioner(Partitioner):
    def __init__(self, db_path):
        self.db_path = db_path

    def partition(self, partition_policy, access_control_policy):
        db_buckets = {}

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
                principal_id = access_control_policy(table, row)
                if principal_id == None:
                    continue
                db_bucket_id = partition_policy(principal_id)

                # Create empty SQLite file if it does not exist yet
                if db_bucket_id not in db_buckets:
                    # TODO: generalize so that user can specify name
                    db_bucket_path = str(db_bucket_id) + self.db_path
                    bucket_con = sqlite3.connect(db_bucket_path)
                    bucket_cur = bucket_con.cursor()
                    for table_schema in schema:
                        # sqlite_sequence table gets created automatically; error is thrown if created manually
                        if "sqlite_sequence" not in table_schema[0]: 
                            bucket_cur.execute(table_schema[0])
                    db_buckets[db_bucket_id] = (bucket_con, bucket_cur)
                else:
                    bucket_cur = db_buckets[db_bucket_id][0]

                # Then, add row to its respective bucket DB
                bucket_cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join('?' * len(row))});", row)

        # Close connections to DB
        for bucket_con, _ in db_buckets.values():
            bucket_con.commit()
            bucket_con.close()
        con.close()


# For testing, delete later

def create_messages_db(db_name):
    """Create an SQLite database with a messages table."""
    # Connect to the SQLite database (it will be created if it doesn't exist)
    conn = sqlite3.connect(db_name)

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Create the messages table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid INTEGER,
            from_me INTEGER,
            content TEXT
        )
    ''')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def insert_message(db_name, gid, from_me, content):
    """Insert a new message into the messages table."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO messages (gid, from_me, content)
        VALUES (?, ?, ?)
    ''', (gid, from_me, content))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    import os
    import glob

    file_type = "*.db"

    for file_path in glob.glob(file_type):
        os.remove(file_path)

    db_name = 'messages.db'
    create_messages_db(db_name)

    # Params: db_name, gid, from_me, content
    insert_message(db_name, 1, 1, 'Hello, World!')
    insert_message(db_name, 1, 1, 'Hello, World!')
    insert_message(db_name, 2, 1, 'Hello, World!')
    insert_message(db_name, 7, 1, 'Hello, World!')
    insert_message(db_name, 7, 1, 'Hello, World!')
    insert_message(db_name, 7, 1, 'Hello, World!')

    partitioner = SQLiteSimplePartitioner(db_name)

    def access(table, row):
        if table[0]=="messages":
            return row[1]
        else:
            return "metadata"
    
    def partition_policy(id):
        return id

    partitioner.partition(partition_policy, access)

