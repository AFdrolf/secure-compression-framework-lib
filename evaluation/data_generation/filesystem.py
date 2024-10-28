import csv
from faker import Faker
import os
from pathlib import Path
import random


from evaluation.util import LONG_TAIL_MODEL_CONSTANTS


FILE_SIZE_BYTES = 4096


def generate_files(number_chats: int, number_files: int, communication_model: str, files_output_loc: Path, csv_output_file: Path):
    number_files_per_chat = generate_number_files_chats(number_chats, number_files, communication_model)
    files_sender = []
    for n in range(number_files_per_chat):
        # Sample a random phone number for the principal who sent this attachment
        principal = random.randint(100000000, 9999999999)
        for m in range(n):
            filename = files_output_loc / f"{n}_{m}"
            with open(filename, 'wb') as f:
                f.write(os.urandom(FILE_SIZE_BYTES))
            from_owner = random.randint(0, 1)
            sender, recipient = ("owner", principal) if from_owner else (principal, "owner")
            files_sender.append((sender, recipient, filename))
    with open(csv_output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(files_sender)


def generate_number_files_chats(number_chats, number_files, communication_model):
    number_files_per_chat = []

    # All chats have the same number of files
    if communication_model == "even":
        for i in range(number_chats):
            r = number_files // number_chats
            number_files_per_chat.append(r + 1 if i < number_files % number_chats else r)

    # All chats have a random number of files
    elif communication_model == "random":
        number_files_per_chat = [1] * number_chats
        allocate_files_randomly_to_chats(number_files_per_chat, number_files - number_chats)

    # Most chats have a few files, while a few chats have many files
    elif communication_model == "long_tail":
        # TODO: some basic checks to make sure that input numbers always make sense (e.g., that there are enough files for low-activity chats to have at least one message)
        high_activity_chats_total = number_chats * LONG_TAIL_MODEL_CONSTANTS["high_activity_chats_percentage"]
        high_activity_files_total = number_files * LONG_TAIL_MODEL_CONSTANTS["high_activity_files_percentage"]
        high_activity_chats_avg_files = high_activity_files_total // high_activity_chats_total
        high_activity_chats_min_files = (
            high_activity_chats_avg_files * LONG_TAIL_MODEL_CONSTANTS["high_activity_chat_min_files"]
        )

        # First, allocate files to the high-activity chats
        files_allocated = 0
        for _ in range(high_activity_chats_total):
            number_files = random.randint(high_activity_chats_min_files, high_activity_chats_avg_files)
            number_files_per_chat.append(number_files)
            files_allocated += number_files
        # If there are still files for the high activity chats left, allocate them randomly to these
        allocate_files_randomly_to_chats(
            number_files_per_chat, high_activity_files_total - files_allocated
        )

        # Then, allocate remaining files to the rest of the chats
        low_activity_chats = [1] * (number_chats - high_activity_chats_total)
        number_files_per_chat += allocate_files_randomly_to_chats(
            low_activity_chats, number_files - high_activity_files_total, high_activity_chats_min_files - 1
        )

    return number_files_per_chat


def allocate_files_randomly_to_chats(chats_list, number_files, max_files_chat=None):
    for _ in range(number_files):
        chat_ix = random.randint(0, len(chats_list) - 1)
        while max_files_chat and max_files_chat <= chats_list[chat_ix]:
            chat_ix = random.randint(0, len(chats_list) - 1)
        chats_list[chat_ix] += 1
    return chats_list