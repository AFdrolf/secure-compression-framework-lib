import os
from pathlib import Path

FILE_SIZE_BYTES = 4096

def generate_files(number_chats: int, number_messages: int, communication_model: str, output_loc: Path):
    for _ in range(number_chats*number_messages):
        file_content = os.urandom(FILE_SIZE_BYTES)