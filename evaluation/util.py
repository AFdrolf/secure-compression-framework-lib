import math
import random
import zlib
from pathlib import Path


LONG_TAIL_MODEL_CONSTANTS = {
    "high_activity_chats_percentage": 0.10,  # Percentage of chats that have high activity
    "high_activity_messages_percentage": 0.80,  # Percentage of total messages sent by the high-activity chats
    "high_activity_chat_min_messages": 0.5,  # To compute the minimum number of messages to be considered a high-activity chat
}

def generate_distribution(n: int, m: int, dist: str) -> list[int]:
    """Generate a distribution of messages/entries for evaluation.

    Args:
    -----
    n: Number of groups
    m: Number of messages/entries
    D: Distribution type

    Returns: a list of ints where each element represents a group and the value represents number of messages/entries in
    that group.
    """
    number_messages_per_chat = []

    # All chats have the same number of messages
    if dist == "even":
        for i in range(n):
            r = m // n
            number_messages_per_chat.append(r + 1 if i < m % n else r)

    # All chats have a random number of messages
    elif dist == "random":
        number_messages_per_chat = [1] * n
        allocate_messages_randomly_to_chats(number_messages_per_chat, m - n)

    # Most chats have a few messages, while a few chats have many messages
    elif dist == "long_tail":
        # TODO: some basic checks to make sure that input numbers always make sense (e.g., that there are enough
        #  messages for low-activity chats to have at least one message)
        high_activity_chats_total = math.floor(n * LONG_TAIL_MODEL_CONSTANTS["high_activity_chats_percentage"])
        high_activity_messages_total = math.floor(m * LONG_TAIL_MODEL_CONSTANTS["high_activity_messages_percentage"])
        high_activity_chats_avg_messages = high_activity_messages_total // high_activity_chats_total
        high_activity_chats_min_messages = (
            math.floor(high_activity_chats_avg_messages * LONG_TAIL_MODEL_CONSTANTS["high_activity_chat_min_messages"])
        )

        # First, allocate messages to the high-activity chats
        messages_allocated = 0
        for _ in range(high_activity_chats_total):
            m = random.randint(high_activity_chats_min_messages, high_activity_chats_avg_messages)
            number_messages_per_chat.append(m)
            messages_allocated += m
        # If there are still messages for the high activity chats left, allocate them randomly to these
        allocate_messages_randomly_to_chats(
            number_messages_per_chat, high_activity_messages_total - messages_allocated
        )

        # Then, allocate remaining messages to the rest of the chats
        low_activity_chats = [1] * (n - high_activity_chats_total)
        number_messages_per_chat += allocate_messages_randomly_to_chats(
            low_activity_chats, m - high_activity_messages_total, high_activity_chats_min_messages - 1
        )

    else:
        raise ValueError(f"Distribution should be 'even', 'random' or 'long_tail'")

    return number_messages_per_chat


def allocate_messages_randomly_to_chats(chats_list, number_messages, max_messages_chat=None):
    for _ in range(number_messages):
        chat_ix = random.randint(0, len(chats_list) - 1)
        while max_messages_chat and max_messages_chat <= chats_list[chat_ix]:
            chat_ix = random.randint(0, len(chats_list) - 1)
        chats_list[chat_ix] += 1
    return chats_list


def compress_file(in_path: Path, out_path: Path):
    b = in_path.read_bytes()
    c = zlib.compress(b)
    out_path.write_bytes(c)
