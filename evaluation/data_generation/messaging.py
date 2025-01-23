import copy
import csv
import random
import time
from pathlib import Path

from faker import Faker

from evaluation.util import generate_distribution


def generate_messaging_csv(n: int, m: int, dist: str, output_path: Path):
    """Given some parameters, generate a CSV containing messaging data.

    Args:
    -----
    n: Number of messaging groups
    m: Number of messages across entire database
    dist: Distribution of the number of messages for a group ("even", "random" or "long_tail")
    output_path: Output path for generated CSV file

    """
    group_num_messages = generate_distribution(n, m, dist)
    faker = Faker()
    user = "owner"
    rows = [["sender", "recipient", "text", "timestamp"]]

    conversations_path = Path(__file__).parent.parent / "helper_data" / "conversations.txt"
    with conversations_path.open() as f:
        conversations = f.readlines()
    random.shuffle(conversations)

    conversation_index = 0
    groups = [faker.name() for _ in range(len(group_num_messages))]
    groups_to_messages = {groups[i]: group_num_messages[i] for i in range(len(groups))}

    timestamp = int(time.time() - 3.15e7) # One year ago

    while max(groups_to_messages.values()) > 0:
        # Random choice of group weighted by number of messages left to send
        g = random.choices(*zip(*groups_to_messages.items()))[0]
        users = [user, g]

        initiator = random.randint(0, 1)
        added_messages = 0
        for i, m in enumerate(conversations[conversation_index][:-1].split(" \\")):
            sender = users[(initiator + i) % 2]
            recipient = users[(initiator + i + 1) % 2]
            rows.append([sender, recipient, m, timestamp])
            timestamp += random.randint(1, 10 * 60)
            added_messages += 1
        groups_to_messages[g] = max(groups_to_messages[g]-added_messages, 0)
        conversation_index += 1
        timestamp += random.randint(1, 2 * 60 * 60)

    with output_path.open("w") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
