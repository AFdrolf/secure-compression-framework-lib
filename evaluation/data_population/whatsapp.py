import csv
from dataclasses import dataclass
from pathlib import Path
import secrets
import sqlite3


SQL = {
    "chat_table_insert": "INSERT INTO chat(jid_row_id) VALUES (?,)",
    "jid_table_insert": "INSERT INTO jid(user,server) VALUES (?, ?)",
    "message_table_insert": "INSERT INTO message(chat_row_id,from_me,key_id,sender_jid_row_id,timestamp,received_timestamp,text_data) VALUES (?, ?, ?, ?, ?, ?, ?)",
}
WA_SCHEMA_PATH = Path(__file__).parent.parent / "helper_data" / "whatsapp_schema.sql"


@dataclass
class SimpleWhatsAppMessage:
    from_owner: bool
    interacting_principal_id: str
    text: str
    timestamp: int  # Milliseconds


def insert_message_in_db(cur: sqlite3.Cursor, whatsapp_message_obj: SimpleWhatsAppMessage, seen_principals: dict):
    # If principal is new, insert row into 'jid' and 'chats' table
    if whatsapp_message_obj.interacting_principal_id not in seen_principals:
        jid_row_id = len(seen_principals) + 1
        chat_row_id = len(seen_principals) + 1
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
    received_timestamp = timestamp - timestamp % 1000 if not from_me else 0
    text_data = whatsapp_message_obj.text
    cur.execute(
        SQL["message_table_insert"],
        (chat_row_id, from_me, key_id, sender_jid_row_id, timestamp, received_timestamp, text_data),
    )


def parse_transcript_csv(chats_csv: Path):
    with open(chats_csv, mode="r", newline="", encoding="utf-8") as f:
        chats_csv_reader = csv.reader(f)
        for row in chats_csv_reader:
            yield row


def create_empty_db_from_schema(schema_path: Path, db_path: Path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    with open(schema_path, "r") as f:
        sql_statements = f.read()
    cur.executescript(sql_statements)

    # Add default values present in jid and chat tables
    cur.execute(SQL["jid_table_insert"], ("status", "broadcast"))
    cur.execute(SQL["jid_table_insert"], (0, "s.whatsapp.net"))
    cur.execute(SQL["jid_table_insert"], (None, "status_me"))
    cur.execute(SQL["chats_table_insert"], (1,))

    return con, cur


def generate_whatsapp_sqlite(chats_csv: Path, db_output_path: Path):
    con, cur = create_empty_db_from_schema(WA_SCHEMA_PATH, db_output_path)
    seen_principals = dict()
    for csv_row in parse_transcript_csv(chats_csv):
        sender, recipient, text, timestamp = csv_row
        from_owner, interacting_principal_id = (1, recipient) if sender == "owner" else (0, sender)
        whatsapp_message_obj = SimpleWhatsAppMessage(from_owner, interacting_principal_id, text, timestamp)
        insert_message_in_db(cur, whatsapp_message_obj, seen_principals)
    con.commit()
    con.close()
