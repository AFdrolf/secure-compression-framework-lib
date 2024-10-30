import random

from evaluation.util import generate_distribution

LLM_PROMPT = ""


def generate_chats_llm_prompt(number_chats: int, number_messages: int, communication_model: str, csv_output_file):
    """Given some seed parameters, generates the prompt to feed to an LLM for generating a transcript of chats.

    This function corresponds to a single iteration of the evaluation.

    Args:
    -----
    number_chats: Number of chats
    number_messages: Total number of messages sent across all chats
    """
    number_messages_per_chat = generate_distribution(number_chats, number_messages, communication_model)

    # NOTE: For testing purposes; eventually replace with call to LLM
    import csv
    import time
    from pathlib import Path

    from faker import Faker

    message_generator = Faker()
    conversations = []
    for n in range(len(number_messages_per_chat)):
        # Sample a random phone number for the principal in this chat
        principal = random.randint(100000000, 9999999999)
        # For every message in this chat, flip a coin to determine if it was sent or received, and then sample a random timestamp (in the past week) and a random text
        for _ in range(n):
            from_owner = random.randint(0, 1)
            timestamp = int(time.time() * 1000) - random.randint(
                0, 604800000
            )  # 604800000 is the number of milliseconds in a week
            text = message_generator.sentence()
            sender, recipient = ("owner", principal) if from_owner else (principal, "owner")
            conversations.append((sender, recipient, text, timestamp))
    conversations.sort(key=lambda message: message[3])
    with open(csv_output_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(conversations)
