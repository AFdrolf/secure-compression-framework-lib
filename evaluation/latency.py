import argparse
import functools
import timeit
from pathlib import Path

from evaluation.data_generation.keepass import generate_keepass_csv
from evaluation.data_generation.messaging import generate_messaging_csv
from evaluation.data_population.keepass import generate_keepass_xml
from evaluation.data_population.whatsapp import generate_whatsapp_sqlite
from evaluation.util import compress_file, decompress_file
from secure_compression_framework_lib.end_to_end.compress_sqlite_advanced import compress_sqlite_advanced, \
    decompress_sqlite_advanced
from secure_compression_framework_lib.end_to_end.compress_xml_advanced import (
    compress_xml_advanced_by_element,
    decompress_xml_advanced_by_element,
)
from secure_compression_framework_lib.partitioner.access_control import Principal, \
    generate_attribute_based_partition_policy
from tests.test_partitioner_sqlite import gid_as_principal_access_control_policy
from tests.test_partitioner_xml import example_group_uuid_as_principal_keepass_sample_xml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where evaluation files and stats will be output to", type=Path)
    parser.add_argument("trials", help="Number of trials", type=int)
    parser.add_argument("partitioner", help="Partitioner to test (keepass/whatsapp)", type=str)
    args = parser.parse_args()

    csv_path = args.output_dir / f"timing.csv"
    if args.partitioner == "keepass":
        out_path = args.output_dir / f"timing.xml"
        generate_keepass_csv(10, 200, "even", True, csv_path)
        generate_keepass_xml(csv_path, args.output_dir, True)
    else:
        out_path = args.output_dir / f"timing.db"
        generate_messaging_csv(100, 200000, "even", csv_path)
        generate_whatsapp_sqlite(csv_path, out_path)

    compress_path = args.output_dir / f"timing.gz"
    compress_func = functools.partial(compress_file, out_path, compress_path)

    compress_time = timeit.timeit("compress_func()", setup="from __main__ import compress_func", number=args.trials)

    decompress_func = functools.partial(decompress_file, compress_path)

    decompress_time = timeit.timeit("decompress_func()", setup="from __main__ import decompress_func", number=args.trials)

    partition_path = args.output_dir / f"timing.gz.safe"

    if args.partitioner == "keepass":
        def safe_compress_func():
            partition_compressed_bytes = compress_xml_advanced_by_element(
                out_path, example_group_uuid_as_principal_keepass_sample_xml
            )
            partition_path.write_bytes(partition_compressed_bytes)
    else:
        def safe_compress_func():
            partition_compressed_bytes = compress_sqlite_advanced(out_path, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid"))
            for db in args.output_dir.glob(f"*_timing.db"):
                db.unlink()
            partition_path.write_bytes(partition_compressed_bytes)

    safe_compress_time = timeit.timeit(
        safe_compress_func, setup="from __main__ import safe_compress_func", number=args.trials
    )

    if args.partitioner == "keepass":
        def safe_decompress_func():
            b = partition_path.read_bytes()
            decompress_xml_advanced_by_element(b)
    else:
        def safe_decompress_func():
            b = partition_path.read_bytes()
            decompress_sqlite_advanced(b)

    safe_decompress_time = timeit.timeit(
        safe_decompress_func, setup="from __main__ import safe_decompress_func", number=args.trials
    )

    print(f"Normal compress time: {compress_time/args.trials*1000}")
    print(f"Safe compress time: {safe_compress_time/args.trials*1000}")
    print(f"Normal decompress time: {decompress_time/args.trials*1000}")
    print(f"Safe decompress time: {safe_decompress_time/args.trials*1000}")
