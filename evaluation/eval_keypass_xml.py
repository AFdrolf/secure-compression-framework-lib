import argparse
import zlib
from pathlib import Path

from evaluation.data_populator.keepass_xml.keepass_xml_populator import generate_keepass_xml
from secure_compression_framework_lib.end_to_end.compress_xml_advanced import compress_xml_advanced_by_element
from tests.test_partitioner_xml_advanced import example_group_uuid_as_principal_keepass_sample_xml


def compress_file(in_path: Path, out_path: Path):
    b = in_path.read_bytes()
    c = zlib.compress(b)
    out_path.write_bytes(c)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "output_dir", help="Directory where raw xml and compressed versions will be output to", type=Path
    )
    args = parser.parse_args()
    for n in range(1, 5):
        for dist in ["shallow"]:
            et = generate_keepass_xml(n, dist)
            path = args.output_dir / f"{n}_{dist}.xml"
            et.write(path)
            partition_compressed_bytes, stream_switch = compress_xml_advanced_by_element(
                path, example_group_uuid_as_principal_keepass_sample_xml
            )
            partition_path = args.output_dir / f"{n}_{dist}.xml.gz.safe"
            partition_path.write_bytes(partition_compressed_bytes)
            compress_file(path, args.output_dir / f"{n}_{dist}.xml.gz")
