import csv
from dataclasses import dataclass
import sqlite3

SQL = {
    "chat_table_insert": "INSERT INTO chat(jid_row_id) VALUES (?)",
    "jid_table_insert": "INSERT INTO jid(user,server) VALUES (?, ?)"
}

@dataclass
class SimpleWhatsAppMessage:

    from_owner: bool
    interacting_principal_id: str
    timestamp: int


def create_empty_db_from_schema(db_path, schema_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    with open(schema_path, 'r') as f:
        sql_statements = f.read()
    cur.executescript(sql_statements)

    # Add default values present in jid and chat tables
    cur.execute(SQL["jid_table_insert"], ("status", "broadcast"))
    cur.execute(SQL["jid_table_insert"], (0, "s.whatsapp.net"))
    cur.execute(SQL["jid_table_insert"], (None, "status_me"))
    cur.execute(SQL["chats_table_insert"], (1,))


    con.commit()

    return con, cur


def insert_message_in_db(con, cur, whatsapp_message_obj, seen_principals=set()):
    if whatsapp_message_obj.interacting_principal_id not in seen_principals:
        # If principal is new, create entry in 'jid' and 'chats' table
        jid_row_id = cur.execute("SELECT max(_id) FROM jid").fetchone()[0] + 1
        cur.execute(SQL["jid_table_insert"], (whatsapp_message_obj.interacting_principal_id, "s.whatsapp.net"))
        cur.execute(SQL["chats_table_insert"], (jid_row_id,))

    else:
        # Since principal is not new, just update their existing 'chats' row
        pass
    



    # Update 'chat' table
    chat_id = None
    # Update 'message' table
    # Update into 'message_details' table
    # Update into messages_ftsv2
    # Update 'props' table
    # Update 'sqlite_sequence' table
    # Update receipt_user
    return


def parse_transcript_csv(chats_csv):
    with open(chats_csv, mode='r', newline='', encoding='utf-8') as f:
        chats_csv_reader = csv.reader(f)
        for row in chats_csv_reader:
            yield row
    return


def main(db_path, schema_path, chats_csv):
    con, cur = create_empty_db_from_schema(db_path, schema_path)
    for csv_row in parse_transcript_csv(chats_csv):
        whatsapp_message_obj = SimpleWhatsAppMessage(*csv_row)
        insert_message_in_db(con, cur, whatsapp_message_obj)
    con.close()

