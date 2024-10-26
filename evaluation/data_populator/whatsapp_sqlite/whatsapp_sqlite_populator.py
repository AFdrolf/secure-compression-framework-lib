import csv
from dataclasses import dataclass
import secrets
import sqlite3

SQL = {
    "chat_table_insert": "INSERT INTO chat(jid_row_id) VALUES (?)",
    "jid_table_insert": "INSERT INTO jid(user,server) VALUES (?, ?)",
    "message_table_insert": "INSERT INTO message(chat_row_id,from_me,key_id,sender_jid_row_id,timestamp,received_timestamp,text_data) VALUES (?, ?)"
}

@dataclass
class SimpleWhatsAppMessage:

    from_owner: bool
    interacting_principal_id: str
    text: str
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


def insert_message_in_db(con, cur, whatsapp_message_obj, seen_principals=dict()):
    # If principal is new, insert row into 'jid' and 'chats' table
    if whatsapp_message_obj.interacting_principal_id not in seen_principals:
        jid_row_id = cur.execute("SELECT max(_id) FROM jid").fetchone()[0] + 1
        chat_row_id = cur.execute("SELECT max(_id) FROM chat").fetchone()[0] + 1
        cur.execute(SQL["jid_table_insert"], (whatsapp_message_obj.interacting_principal_id, "s.whatsapp.net"))
        cur.execute(SQL["chats_table_insert"], (jid_row_id,))
        seen_principals[whatsapp_message_obj.interacting_principal_id] = (jid_row_id, chat_row_id)
    else:
        jid_row_id, chat_row_id = seen_principals[whatsapp_message_obj.interacting_principal_id]
    
    # Insert row into 'message' table
    from_me = int(whatsapp_message_obj.from_owner)
    key_id = str(secrets.token_bytes(16).hex().upper())
    sender_jid_row_id = jid_row_id
    timestamp = whatsapp_message_obj.timestamp
    received_timestamp = timestamp - timestamp % 100 if not from_me else 0
    text_data = whatsapp_message_obj.text
    cur.execute(SQL["message_table_insert"], (chat_row_id, from_me, key_id, jid_row_id, timestamp, received_timestamp, text_data))

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

