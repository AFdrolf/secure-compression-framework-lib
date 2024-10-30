import argparse
import csv
from pathlib import Path

from evaluation.data_generation.keepass import generate_keepass_csv
from evaluation.data_population.keepass import generate_keepass_xml
from evaluation.util import compress_file
from secure_compression_framework_lib.end_to_end.compress_xml_advanced import compress_xml_advanced_by_element
from tests.test_partitioner_xml_advanced import example_group_uuid_as_principal_keepass_sample_xml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where evaluation files and stats will be output to", type=Path)
    parser.add_argument("--n", help="Number of total groups in keepass", nargs="+", type=int, default=[1])
    parser.add_argument("--m", help="Number of total entries in keepass", nargs="+", type=int, default=[1])
    parser.add_argument(
        "--dist", help="Communication model for generation of synthetic test data", nargs="+", type=str, default=["even"]
    )
    parser.add_argument(
        "--disable-cleanup", help="Remove csv/keepass/xml files generated for evaluation", action="store_true"
    )
    args = parser.parse_args()
    cleanup = not args.disable_cleanup

    n_list = args.n
    m_list = args.m
    dist_list = args.dist
    random_password = [True, False]

    stats_columns = [["n", "m", "dist", "random_password", "raw_bytes", "compressed_bytes", "safe_compressed_bytes"]]
    for n in n_list:
        for m in m_list:
            for dist in dist_list:
                for rp in random_password:
                    csv_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.csv"
                    xml_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.xml"
                    generate_keepass_csv(n, m, dist, rp, csv_path)
                    generate_keepass_xml(csv_path, args.output_dir, cleanup)
                    partition_compressed_bytes = compress_xml_advanced_by_element(
                        xml_path, example_group_uuid_as_principal_keepass_sample_xml
                    )
                    partition_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.xml.gz.safe"
                    partition_path.write_bytes(partition_compressed_bytes)
                    compress_path = args.output_dir / f"{n}_{m}_{dist}_{rp}.xml.gz"
                    compress_file(xml_path, compress_path)

                    stats_columns.append(
                        [n, m, dist, rp, xml_path.stat().st_size, compress_path.stat().st_size, partition_path.stat().st_size]
                    )

                    if cleanup:
                        csv_path.unlink()
                        xml_path.unlink()
                        partition_path.unlink()
                        compress_path.unlink()

    with Path(args.output_dir / "stats.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerows(stats_columns)
