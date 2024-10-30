import csv
import os
from pathlib import Path
import random


from evaluation.util import generate_distribution

FILE_SIZE_BYTES = 4096


def generate_files(number_chats: int, number_files: int, communication_model: str, files_output_loc: Path, csv_output_file: Path):
    number_files_per_chat = generate_distribution(number_chats, number_files, communication_model)
    files_sender = []
    for n in range(len(number_files_per_chat)):
        # Sample a random phone number for the principal who sent this attachment
        principal = random.randint(100000000, 9999999999)
        for m in range(n):
            filename = files_output_loc / f"{n}_{m}"
            with open(filename, 'wb') as f:
                f.write(os.urandom(FILE_SIZE_BYTES))
            from_owner = random.randint(0, 1)
            sender, recipient = ("owner", principal) if from_owner else (principal, "owner")
            files_sender.append((sender, recipient, filename))
    with open(csv_output_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(files_sender)
