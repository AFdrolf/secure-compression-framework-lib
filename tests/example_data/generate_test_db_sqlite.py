import glob
import os
import sqlite3


def create_messages_db(db_name):
    """Create an SQLite database with a messages table."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid INTEGER,
            from_me INTEGER,
            content TEXT
        )
    """)

    conn.commit()
    conn.close()


def insert_message(db_name, gid, from_me, content):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO messages (gid, from_me, content)
        VALUES (?, ?, ?)
    """,
        (gid, from_me, content),
    )

    conn.commit()
    conn.close()


def generate_test_db_sqlite(output_dir, delete_old=True):
    if delete_old:
        path = output_dir + "/*.db"
        for file in glob.glob(path):
            os.remove(file)

    db_name = output_dir + "/test_messages.db"
    create_messages_db(db_name)

    # Params: db_name, gid, from_me, content
    insert_message(db_name, 1, 1, "Hello, World!")
    insert_message(db_name, 1, 1, "Hello, World!")
    insert_message(db_name, 2, 1, "Hello, World!")
    insert_message(db_name, 7, 1, "Hello, World!")
    insert_message(db_name, 7, 1, "Hello, World!")
    insert_message(db_name, 7, 1, "Hello, World!")

    return db_name

# For testing; delete later.
db_name = "/Users/andresfg/Desktop/Cornell/Research/secure-processing-framework-project/secure-compression-framework-lib/tests/example_data/test_messages.db"
con = sqlite3.connect(db_name)
cur = con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()
for table in tables:
    table_name = table[0]
    cur.execute(f"SELECT * FROM {table_name};")
    for row in cur:
        print(row, type(row))
con.close()