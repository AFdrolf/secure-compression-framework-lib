import argparse
from pathlib import Path

from evaluation.data_generation.messaging import generate_chats_LLM_prompt
from evaluation.data_population.whatsapp import generate_whatsapp_sqlite
from evaluation.util import compress_file
from secure_compression_framework_lib.end_to_end.compress_sqlite_simple import compress_sqlite_simple

OUTPUT_FILENAMES = {
    "messaging_data_csv": "messaging_data.csv",
    "db": "messaging_data.db",
    "compressed_db": "messaging_data.db",
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where all output files will be saved", type=Path)
    parser.add_argument("number_chats", help="Number of total chats in database", type=int)
    parser.add_argument("number_messages", help="Number of total messages in database", type=int)
    parser.add_argument(
        "communication_model", help="Communication model for generation of synthetic test data", type=int
    )
    args = parser.parse_args()

    # First, generate a transcript of chats of synthetic test data, based on the input parameters
    chats_csv = parser.output_dir / OUTPUT_FILENAMES["messaging_data_csv"]
    generate_chats_LLM_prompt(args.number_chats, args.number_messages, args.communication_model, chats_csv)

    # Then, generate and populate a WhatsApp database with this transcript
    db_path = parser.output_dir / OUTPUT_FILENAMES["db"]
    generate_whatsapp_sqlite(chats_csv, db_path)

    # Then, compress the database with both regular zlib and with our framework
    compressed_db_path = parser.output_dir / OUTPUT_FILENAMES["compressed_db"]
    compress_file(db_path, compressed_db_path)

    # TODO fill these in. How do we pass these as inputs?
    access_control_policy, partition_policy = None, None
    compressed_db_buckets = compress_sqlite_simple(db_path, access_control_policy, partition_policy)

    # Lastly, compare and report on the difference in size
    compressed_db_size = compressed_db_path.stat().st_size
    compressed_db_buckets_total_size = len(compressed_db_buckets)
    print(f"Size of compressed database: {compressed_db_size}")
    print(f"Total combined size of compressed buckets: {compressed_db_buckets_total_size}")
    print(f"Resulting overhead: {compressed_db_buckets_total_size - compressed_db_size}")
