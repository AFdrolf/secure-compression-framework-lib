import sqlite3

def create_empty_db_from_schema(db_path, schema_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    with open(schema_path, 'r') as f:
        sql_statements = f.read()
    cur.executescript(sql_statements)

    return con, cur


def insert_message_in_db(con, cur, ):
    # Update 'chats' table
    # Update 'message' table
    # Update into 'message_details' table
    # Update into messages_ftsv2
    # Update 'props' table
    # Update 'sqlite_sequence' table
    return


def parse_transcript_csv(csv_file):
    pass
