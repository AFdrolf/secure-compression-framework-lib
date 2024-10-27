import argparse
from pathlib import Path

from evaluation.data_populator.whatsapp_sqlite.whatsapp_sqlite_populator import generate_whatsapp_sqlite
from evaluation.test_data_generator.messaging_data_parameters_generator import generate_chats_LLM_prompt


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "output_dir", help="Directory where raw xml and compressed versions will be output to", type=Path
    )
    args = parser.parse_args()