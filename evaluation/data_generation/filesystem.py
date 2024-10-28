import csv
from faker import Faker
import os
from pathlib import Path
import random


from evaluation.util import LONG_TAIL_MODEL_CONSTANTS


FILE_SIZE_BYTES = 4096


def generate_files(number_chats: int, number_messages: int, communication_model: str, files_output_loc: Path, csv_output_file: Path):
    number_files_per_chat = generate_number_files_chats(number_chats, number_messages, communication_model)
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


def generate_number_files_chats(number_chats, number_messages, communication_model):
    number_messages_per_chat = []

    # All chats have the same number of messages
    if communication_model == "even":
        for i in range(number_chats):
            r = number_messages // number_chats
            number_messages_per_chat.append(r + 1 if i < number_messages % number_chats else r)

    # All chats have a random number of messages
    elif communication_model == "random":
        number_messages_per_chat = [1] * number_chats
        allocate_files_randomly_to_chats(number_messages_per_chat, number_messages - number_chats)

    # Most chats have a few messages, while a few chats have many messages
    elif communication_model == "long_tail":
        # TODO: some basic checks to make sure that input numbers always make sense (e.g., that there are enough messages for low-activity chats to have at least one message)
        high_activity_chats_total = number_chats * LONG_TAIL_MODEL_CONSTANTS["high_activity_chats_percentage"]
        high_activity_messages_total = number_messages * LONG_TAIL_MODEL_CONSTANTS["high_activity_messages_percentage"]
        high_activity_chats_avg_messages = high_activity_messages_total // high_activity_chats_total
        high_activity_chats_min_messages = (
            high_activity_chats_avg_messages * LONG_TAIL_MODEL_CONSTANTS["high_activity_chat_min_messages"]
        )

        # First, allocate messages to the high-activity chats
        messages_allocated = 0
        for _ in range(high_activity_chats_total):
            number_messages = random.randint(high_activity_chats_min_messages, high_activity_chats_avg_messages)
            number_messages_per_chat.append(number_messages)
            messages_allocated += number_messages
        # If there are still messages for the high activity chats left, allocate them randomly to these
        allocate_files_randomly_to_chats(
            number_messages_per_chat, high_activity_messages_total - messages_allocated
        )

        # Then, allocate remaining messages to the rest of the chats
        low_activity_chats = [1] * (number_chats - high_activity_chats_total)
        number_messages_per_chat += allocate_files_randomly_to_chats(
            low_activity_chats, number_messages - high_activity_messages_total, high_activity_chats_min_messages - 1
        )

    return number_messages_per_chat


def allocate_files_randomly_to_chats(chats_list, number_messages, max_messages_chat=None):
    for _ in range(number_messages):
        chat_ix = random.randint(0, len(chats_list) - 1)
        while max_messages_chat and max_messages_chat <= chats_list[chat_ix]:
            chat_ix = random.randint(0, len(chats_list) - 1)
        chats_list[chat_ix] += 1
    return chats_list