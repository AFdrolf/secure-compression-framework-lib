import argparse
from pathlib import Path

from evaluation.data_generation.filesystem import generate_files
from evaluation.data_population.filesystem import generate_filesystem_and_db
from secure_compression_framework_lib.end_to_end.dedup_files import dedup_files


OUTPUT_FILENAMES = {
    "messaging_data_csv": "messaging_data.csv",
    "db": "messaging_data.db",
    "compressed_db": "messaging_data.db",
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where all output files will be saved", type=Path)
    parser.add_argument("number_chats", help="Number of total chats", type=int)
    parser.add_argument("number_files", help="Number of total files to dedup", type=int)
    parser.add_argument(
        "communication_model", help="Communication model for generation of synthetic test data", type=int
    )
    args = parser.parse_args()

    # First, generate synthetic files, based on the input parameters
    files_csv = parser.output_dir / OUTPUT_FILENAMES["files_data_csv"]
    files_loc = parser.output_dir / OUTPUT_FILENAMES["files_loc"]
    generate_files(args.number_chats, args.number_messages, args.communication_model, files_loc, files_csv)

    # Then, generate and populate a WhatsApp database containing information about the sender and recipient of the files
    db_path = parser.output_dir / OUTPUT_FILENAMES["db"]
    generate_filesystem_and_db(files_csv, db_path)

    # Then, deduplicate files with both regular dedup and with our framework
    compressed_db_path = parser.output_dir / OUTPUT_FILENAMES["compressed_db"]
    # Change to single stream
    dedup_files(db_path, compressed_db_path)

    # TODO fill these in. How do we pass these as inputs?
    access_control_policy, partition_policy = None, None
    compressed_db_buckets = dedup_files(db_path, access_control_policy, partition_policy)

    # Change to dedup instead of compression
    compressed_db_size = compressed_db_path.stat().st_size
    compressed_db_buckets_total_size = len(compressed_db_buckets)
    print(f"Size of compressed database: {compressed_db_size}")
    print(f"Total combined size of compressed buckets: {compressed_db_buckets_total_size}")
    print(f"Resulting overhead: {compressed_db_buckets_total_size - compressed_db_size}")
