import argparse
import csv
import functools
import timeit
from pathlib import Path
from time import sleep

import psutil

from evaluation.data_generation.keepass import generate_keepass_csv
from evaluation.data_generation.messaging import generate_messaging_csv
from evaluation.data_population.keepass import generate_keepass_xml
from evaluation.data_population.whatsapp import generate_whatsapp_sqlite
from evaluation.util import compress_file, decompress_file
from injection_attacks_mitigation_framework.end_to_end.compress_sqlite_advanced import compress_sqlite_advanced, \
    decompress_sqlite_advanced, merge_bucketed_data
from injection_attacks_mitigation_framework.end_to_end.compress_xml_advanced import (
    compress_xml_advanced_by_element,
    decompress_xml_advanced_by_element,
)
from injection_attacks_mitigation_framework.multi_stream.compress import MSCompressor, ZlibCompressionStream
from injection_attacks_mitigation_framework.partitioner.access_control import Principal, \
    generate_attribute_based_partition_policy, basic_partition_policy
from injection_attacks_mitigation_framework.partitioner.types.sqlite_advanced import SQLiteAdvancedPartitioner
from injection_attacks_mitigation_framework.partitioner.types.xml_advanced import XmlAdvancedPartitioner
from tests.test_partitioner_sqlite import gid_as_principal_access_control_policy
from tests.test_partitioner_xml import example_group_uuid_as_principal_keepass_sample_xml

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", help="Directory where evaluation files and stats will be output to", type=Path)
    parser.add_argument("trials", help="Number of trials", type=int)
    parser.add_argument("partitioner", help="Partitioner to test (keepass/whatsapp)", type=str)
    parser.add_argument("--n", help="Number of buckets", nargs="+", type=int, default=[1])
    parser.add_argument("--m", help="Number of messages/passwords", nargs="+", type=int, default=[1])
    args = parser.parse_args()

    timing_columns = [
        "n",
        "m",
        "dist",
        "trial",
        "normal_compress",
        "normal_compress_cpu",
        "normal_decompress",
        "normal_decompress_cpu",
        "safe_partition",
        "safe_partition_cpu",
        "safe_compress",
        "safe_compress_cpu",
        "safe_decompress",
        "safe_decompress_cpu",
        "baseline_cpu"
    ]

    n_list = args.n
    m_list = args.m
    csv_path = args.output_dir / f"timing.csv"

    with Path(args.output_dir / f"{args.partitioner}_timing_results.csv").open("w") as f:
        writer = csv.writer(f)
        writer.writerow(timing_columns)
        for n in n_list:
            for m in m_list:
                if args.partitioner == "keepass":
                    out_path = args.output_dir / f"timing.xml"
                    generate_keepass_csv(n, m, "even", True, csv_path)
                    generate_keepass_xml(csv_path, args.output_dir, True)
                else:
                    out_path = args.output_dir / f"timing.db"
                    if out_path.exists():
                        out_path.unlink()
                    generate_messaging_csv(n, m, "even", csv_path)
                    generate_whatsapp_sqlite(csv_path, out_path)

                compress_path = args.output_dir / f"timing.gz"
                compress_func = functools.partial(compress_file, out_path, compress_path)

                psutil.cpu_percent()
                baseline_cpu = []
                for _ in range(args.trials):
                    sleep(3)
                    baseline_cpu.append(psutil.cpu_percent())

                compress_time = []
                compress_cpu = []
                psutil.cpu_percent()
                for _ in range(args.trials):
                    compress_time.append(timeit.timeit("compress_func()", setup="from __main__ import compress_func", number=1))
                    compress_cpu.append(psutil.cpu_percent())

                decompress_func = functools.partial(decompress_file, compress_path)


                decompress_time = []
                decompress_cpu = []
                psutil.cpu_percent()
                for _ in range(args.trials):
                    decompress_time.append(timeit.timeit("decompress_func()", setup="from __main__ import decompress_func", number=1))
                    decompress_cpu.append(psutil.cpu_percent())

                partition_path = args.output_dir / f"timing.gz.safe"

                if args.partitioner == "keepass":
                    def safe_partition_func():
                        partitioner = XmlAdvancedPartitioner(out_path, example_group_uuid_as_principal_keepass_sample_xml, basic_partition_policy)
                        return partitioner.partition()
                    def safe_compress_func(bucketed_data):
                        msc = MSCompressor(ZlibCompressionStream)
                        for bucket, data in bucketed_data:
                            msc.compress(bucket, data)
                        out = msc.finish()
                        partition_path.write_bytes(out)
                else:
                    def safe_partition_func():
                        partitioner = SQLiteAdvancedPartitioner(out_path, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid"))
                        bucketed_data = partitioner.partition()
                        merged_bucketed_data = merge_bucketed_data(bucketed_data)
                        return merged_bucketed_data
                    def safe_compress_func(merged_bucketed_data):
                        msc = MSCompressor(ZlibCompressionStream, stream_switch_delimiter=b"[|\\")
                        for bucket, data in merged_bucketed_data:
                            msc.compress(bucket, data)
                        out = msc.finish()
                        partition_path.write_bytes(out)

                bd = safe_partition_func()
                safe_partition_time = []
                safe_partition_cpu = []
                psutil.cpu_percent()
                for _ in range(args.trials):
                    safe_partition_time.append(timeit.timeit(
                        safe_partition_func, setup="from __main__ import safe_partition_func", number=1
                    ))
                    safe_partition_cpu.append(psutil.cpu_percent())

                safe_compress_time = []
                safe_compress_cpu = []
                psutil.cpu_percent()
                for _ in range(args.trials):
                    safe_compress_time.append(timeit.timeit(
                        functools.partial(safe_compress_func, bd), setup="from __main__ import safe_compress_func", number=1
                    ))
                    safe_compress_cpu.append(psutil.cpu_percent())

                if args.partitioner == "keepass":
                    def safe_decompress_func():
                        b = partition_path.read_bytes()
                        decompress_xml_advanced_by_element(b)
                else:
                    def safe_decompress_func():
                        b = partition_path.read_bytes()
                        decompress_sqlite_advanced(b)

                safe_decompress_time = []
                safe_decompress_cpu = []
                psutil.cpu_percent()
                for _ in range(args.trials):
                    safe_decompress_time.append(timeit.timeit(
                        safe_decompress_func, setup="from __main__ import safe_decompress_func", number=1
                    ))
                    safe_decompress_cpu.append(psutil.cpu_percent())

                for i in range(args.trials):
                    writer.writerow(
                        [
                            n,
                            m,
                            "even",
                            i,
                            compress_time[i],
                            compress_cpu[i],
                            decompress_time[i],
                            decompress_cpu[i],
                            safe_partition_time[i],
                            safe_partition_cpu[i],
                            safe_compress_time[i],
                            safe_compress_cpu[i],
                            safe_decompress_time[i],
                            safe_decompress_cpu[i],
                            baseline_cpu[i]
                        ]
                    )
