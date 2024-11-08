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
    user = faker.name()
    rows = [["sender", "recipient", "text", "timestamp"]]

    conversations_path = Path(__file__).parent.parent / "helper_data" / "conversations.txt"
    with conversations_path.open() as f:
        conversations = f.readlines()
    random.shuffle(conversations)

    for c, num_messages in enumerate(group_num_messages):
        users = [user, faker.name()]
        added_messages = 0
        while added_messages < num_messages:
            timestamp = int(time.time() - random.randint(0, int(3.15e7)))
            initiator = random.randint(0, 1)
            for i, m in enumerate(conversations[c][:-1].split(" \\")):
                sender = users[(initiator + i) % 2]
                recipient = users[(initiator + i + 1) % 2]
                rows.append([sender, recipient, m, timestamp])
                timestamp += random.randint(1, 10 * 60)
                added_messages += 1
                if added_messages > num_messages:
                    break

    with output_path.open("w") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


if __name__ == "__main__":
    generate_messaging_csv(
        2, 10, "even", Path("/Users/sjb373/data/secure-compression-framework-lib/sjb_local/messages.csv")
    )
