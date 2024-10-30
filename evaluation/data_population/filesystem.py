import csv
from dataclasses import dataclass
from pathlib import Path
import sqlite3


SQL = {
    "chat_table_insert": "INSERT INTO chat(jid_row_id) VALUES (?,)",
    "jid_table_insert": "INSERT INTO jid(user) VALUES (?,)",
    "message_media_table_insert": "INSERT INTO message_media(chat_row_id, file_paths) VALUES (?, ?, ?)",
}
WA_SCHEMA_PATH = Path(__file__).parent.parent / "helper_data" / "whatsapp_schema.sql"


@dataclass
class SimpleWhatsAppAttachment:
    from_owner: bool
    interacting_principal_id: str
    filename: str


def insert_attachment_info_in_db(
    cur: sqlite3.Cursor, whatsapp_attach_obj: SimpleWhatsAppAttachment, seen_principals: dict
):
    if whatsapp_attach_obj.interacting_principal_id not in seen_principals:
        jid_row_id = len(seen_principals) + 1
        chat_row_id = len(seen_principals) + 1
        cur.execute(SQL["jid_table_insert"], (whatsapp_attach_obj.interacting_principal_id,))
        cur.execute(SQL["chats_table_insert"], (jid_row_id,))
        seen_principals[whatsapp_attach_obj.interacting_principal_id] = chat_row_id
    else:
        chat_row_id = seen_principals[whatsapp_attach_obj.interacting_principal_id]

    cur.execute(SQL["message_media_table_insert"], (chat_row_id, whatsapp_attach_obj.filename))


def generate_filesystem_and_db(files_sender_csv: Path, db_output_path: Path):
    con = sqlite3.connect(db_output_path)
    cur = con.cursor()

    with open(WA_SCHEMA_PATH, "r") as f:
        sql_statements = f.read()
    cur.executescript(sql_statements)

    with open(files_sender_csv, mode="r", newline="", encoding="utf-8") as f:
        chats_csv_reader = csv.reader(f)
        seen_principals = dict()
        for csv_row in chats_csv_reader:
            sender, recipient, filename = csv_row
            from_owner, interacting_principal_id = (1, recipient) if sender == "owner" else (0, sender)
            whatsapp_attach_obj = SimpleWhatsAppAttachment(from_owner, interacting_principal_id, filename)
            insert_attachment_info_in_db(cur, whatsapp_attach_obj, seen_principals)
    con.commit()
    con.close()
