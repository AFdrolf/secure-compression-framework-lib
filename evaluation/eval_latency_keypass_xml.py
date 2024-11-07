import argparse
import functools
import timeit
from pathlib import Path

from evaluation.data_generation.keepass import generate_keepass_csv
from evaluation.data_population.keepass import generate_keepass_xml
from evaluation.util import compress_file, decompress_file
from secure_compression_framework_lib.end_to_end.compress_xml_advanced import (
    compress_xml_advanced_by_element,
    decompress_xml_advanced_by_element,
)
from tests.test_partitioner_xml import example_group_uuid_as_principal_keepass_sample_xml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where evaluation files and stats will be output to", type=Path)
    parser.add_argument("trials", help="Number of trials", type=int)
    args = parser.parse_args()

    csv_path = args.output_dir / f"timing.csv"
    xml_path = args.output_dir / f"timing.xml"
    generate_keepass_csv(10, 200, "even", True, csv_path)
    generate_keepass_xml(csv_path, args.output_dir, True)

    compress_path = args.output_dir / f"timing.xml.gz"
    compress_func = functools.partial(compress_file, xml_path, compress_path)

    compress_time = timeit.timeit("compress_func()", setup="from __main__ import compress_func", number=args.trials)

    decompress_func = functools.partial(decompress_file, compress_path)

    decompress_time = timeit.timeit("decompress_func()", setup="from __main__ import decompress_func", number=args.trials)

    partition_path = args.output_dir / f"timing.xml.gz.safe"

    def safe_compress_func():
        partition_compressed_bytes = compress_xml_advanced_by_element(
            xml_path, example_group_uuid_as_principal_keepass_sample_xml
        )
        partition_path.write_bytes(partition_compressed_bytes)

    safe_compress_time = timeit.timeit(
        safe_compress_func, setup="from __main__ import safe_compress_func", number=args.trials
    )

    def safe_decompress_func():
        b = partition_path.read_bytes()
        decompress_xml_advanced_by_element(b)

    safe_decompress_time = timeit.timeit(
        safe_decompress_func, setup="from __main__ import safe_decompress_func", number=args.trials
    )

    print(f"Normal compress time: {compress_time/args.trials*1000}")
    print(f"Safe compress time: {safe_compress_time/args.trials*1000}")
    print(f"Normal decompress time: {decompress_time/args.trials*1000}")
    print(f"Safe decompress time: {safe_decompress_time/args.trials*1000}")
