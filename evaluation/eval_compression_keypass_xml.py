import argparse
import csv
from pathlib import Path

from evaluation.data_generation.keepass import generate_keepass_csv
from evaluation.data_population.keepass import generate_keepass_xml
from evaluation.util import compress_file
from secure_compression_framework_lib.end_to_end.compress_xml_advanced import compress_xml_advanced_by_element
from secure_compression_framework_lib.end_to_end.compress_xml_simple import compress_xml_simple
from secure_compression_framework_lib.partitioner.access_control import basic_partition_policy
from tests.test_partitioner_xml import example_group_uuid_as_principal_keepass_sample_xml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where evaluation files and stats will be output to", type=Path)
    parser.add_argument("--n", help="Number of total groups in keepass", nargs="+", type=int, default=[1])
    parser.add_argument("--m", help="Number of total entries in keepass", nargs="+", type=int, default=[1])
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
    random_password = [True]

    stats_columns = ["n", "m", "dist", "random_password", "raw_bytes", "compressed_bytes", "safe_compressed_bytes_simple", "safe_compressed_bytes_advanced"]

    with Path(args.output_dir / "stats.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(stats_columns)
        for n in n_list:
            for m in m_list:
                for dist in dist_list:
                    for rp in random_password:
                        csv_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.csv"
                        xml_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.xml"
                        generate_keepass_csv(n, m, dist, rp, csv_path)
                        generate_keepass_xml(csv_path, args.output_dir, cleanup)

                        compress_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.xml.gz"
                        compress_file(xml_path, compress_path)

                        partition_compressed_bytes = compress_xml_simple(
                            xml_path, example_group_uuid_as_principal_keepass_sample_xml, basic_partition_policy
                        )
                        simple_safe_size = 0
                        for i, b in enumerate(partition_compressed_bytes):
                            partition_path = args.output_dir / f"{n}_{m}_{dist}_{rp}_{i}.xml.gz.safe.simple"
                            partition_path.write_bytes(b)
                            simple_safe_size += partition_path.stat().st_size
                            if cleanup:
                                partition_path.unlink()

                        partition_compressed_bytes = compress_xml_advanced_by_element(
                            xml_path, example_group_uuid_as_principal_keepass_sample_xml
                        )
                        partition_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.xml.gz.safe.advanced"
                        partition_path.write_bytes(partition_compressed_bytes)
                        advanced_safe_size = partition_path.stat().st_size
                        if cleanup:
                            partition_path.unlink()

                        writer.writerow(
                            [
                                n,
                                m,
                                dist,
                                rp,
                                xml_path.stat().st_size,
                                compress_path.stat().st_size,
                                simple_safe_size,
                                advanced_safe_size
                            ]
                        )
                        print(f"Finished {n} {m} {dist} {rp}")
                        if cleanup:
                            csv_path.unlink()
                            xml_path.unlink()
                            compress_path.unlink()
