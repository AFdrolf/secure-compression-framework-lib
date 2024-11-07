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


def generate_test_db_sqlite(output_dir):

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
