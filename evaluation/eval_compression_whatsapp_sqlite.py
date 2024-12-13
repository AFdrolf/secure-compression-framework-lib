import argparse
import csv
from pathlib import Path

import sys

from secure_compression_framework_lib.end_to_end.compress_sqlite_advanced import compress_sqlite_advanced
from tests.test_partitioner_sqlite import gid_as_principal_access_control_policy

sys.path.append(sys.path[0] + "/..")

from evaluation.data_generation.messaging import generate_messaging_csv
from evaluation.data_population.whatsapp import generate_whatsapp_sqlite
from evaluation.util import compress_file
from secure_compression_framework_lib.end_to_end.compress_sqlite_simple import compress_sqlite_simple
from secure_compression_framework_lib.partitioner.access_control import generate_attribute_based_partition_policy

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where evaluation files and stats will be output to", type=Path)
    parser.add_argument("--n", help="Number of total groups in Whatsapp", nargs="+", type=int, default=[1])
    parser.add_argument("--m", help="Number of total messages in Whatsapp", nargs="+", type=int, default=[1])
    parser.add_argument(
        "--dist",
        help="Communication model for generation of synthetic test data",
        nargs="+",
        type=str,
        default=["even"],
    )
    # parser.add_argument("--simple", help="Use simple partitioner", action="store_true")
    parser.add_argument(
        "--no-cleanup", help="Remove csv/keepass/xml files generated for evaluation", action="store_true"
    )
    args = parser.parse_args()
    cleanup = not args.no_cleanup

    n_list = args.n
    m_list = args.m
    dist_list = args.dist

    stats_columns = [
        "n",
        "m",
        "dist",
        "raw_bytes",
        "compressed_bytes",
        "safe_compressed_bytes_simple",
        "safe_compressed_bytes_advanced",
    ]

    with Path(args.output_dir / "whatsapp_stats.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(stats_columns)
        for n in n_list:
            for m in m_list:
                for dist in dist_list:
                    # First, generate a transcript of chats of synthetic test data, based on the input parameters
                    chats_csv = args.output_dir / f"{n}_{m}_{dist}.csv"
                    generate_messaging_csv(n, m, dist, chats_csv)

                    # Then, generate and populate a WhatsApp database with this transcript
                    db_path = args.output_dir / f"{n}_{m}_{dist}.db"
                    generate_whatsapp_sqlite(chats_csv, db_path)

                    # Then, compress the database with both regular zlib and with our framework
                    compressed_db_buckets = compress_sqlite_simple(db_path, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid"))
                    safe_compressed_db_path = args.output_dir / f"{n}_{m}_{dist}.db.gz.safe.simple"
                    safe_compressed_db_path.write_bytes(compressed_db_buckets)

                    advanced_compressed_bytes = compress_sqlite_advanced(db_path, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid"))
                    advanced_safe_compressed_path = args.output_dir / f"{n}_{m}_{dist}.db.gz.safe.advanced"
                    advanced_safe_compressed_path.write_bytes(advanced_compressed_bytes)

                    # We do regular compression last because somehow the partitioner slightly changes DB size
                    # Maybe by running queries?
                    compressed_db_path = args.output_dir / f"{n}_{m}_{dist}.db.gz"
                    compress_file(db_path, compressed_db_path)

                    writer.writerow(
                        [
                            n,
                            m,
                            dist,
                            db_path.stat().st_size,
                            compressed_db_path.stat().st_size,
                            safe_compressed_db_path.stat().st_size,
                            advanced_safe_compressed_path.stat().st_size,
                        ]
                    )
                    print(f"Finished {n} {m} {dist}")
                    if cleanup:
                        chats_csv.unlink()
                        db_path.unlink()
                        compressed_db_path.unlink()
                        safe_compressed_db_path.unlink()
                        advanced_safe_compressed_path.unlink()
                        for db in args.output_dir.glob(f"*_{n}_{m}_{dist}.db"):
                            db.unlink()
