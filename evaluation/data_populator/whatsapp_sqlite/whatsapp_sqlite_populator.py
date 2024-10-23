import csv
from dataclasses import dataclass
import sqlite3


@dataclass
class SimpleWhatsAppMessage:

    sender: str
    recipient: str
    timestamp: int


def create_empty_db_from_schema(db_path, schema_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    with open(schema_path, 'r') as f:
        sql_statements = f.read()
    cur.executescript(sql_statements)

    return con, cur


def insert_message_in_db(con, cur, whatsapp_message_obj):
    # Update 'chats' table
    chat_id = None
    # Update 'message' table
    # Update into 'message_details' table
    # Update into messages_ftsv2
    # Update 'props' table
    # Update 'sqlite_sequence' table
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

