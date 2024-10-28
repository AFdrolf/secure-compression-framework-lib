import zlib
from pathlib import Path

LONG_TAIL_MODEL_CONSTANTS = {
    "high_activity_chats_percentage": 0.10,  # Percentage of chats that have high activity
    "high_activity_messages_percentage": 0.80,  # Percentage of total messages sent by the high-activity chats
    "high_activity_chat_min_messages": 0.5,  # To compute the minimum number of messages to be considered a high-activity chat
}


def compress_file(in_path: Path, out_path: Path):
    b = in_path.read_bytes()
    c = zlib.compress(b)
    out_path.write_bytes(c)
