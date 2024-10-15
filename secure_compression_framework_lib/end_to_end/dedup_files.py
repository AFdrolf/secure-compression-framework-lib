from collections.abc import Callable
from pathlib import Path

from secure_compression_framework_lib.multi_stream.dedup import checksum_comparison_function, dedup
from secure_compression_framework_lib.partitioner.access_control import Principal, basic_partition_policy
from secure_compression_framework_lib.partitioner.types.filesystem import FileSystemPartitioner


def dedup_files_by_name(files_dir: Path) -> list[Path]:
    """Implements a basic deduplication scenario where the principal for a file is encoded in the filename.

    Here we imagine the application provides a Python function that extracts the principal from the filename.
    This is the access control policy used by our partitioner

    Args:
        files_dir: Directory containing files to be deduplicated.

    Returns:
        A deduplicated list of files in files_dir. No guarantees about which file will be kept in case of duplicates.
    """

    def example_extract_principal_from_filename(file: Path) -> Principal:
        """Example access control policy function.

        Assumes the principal name is the first field of an underscore separated filename
        """
        return Principal(name=file.name.split("_")[0])

    partitioner = FileSystemPartitioner(files_dir, example_extract_principal_from_filename, basic_partition_policy)
    bucketed_files = partitioner.partition()
    dedup_files = []
    for bucket, files in bucketed_files.items():
        bucket_dedup_files = dedup(checksum_comparison_function, files)
        dedup_files.extend(bucket_dedup_files)
    return dedup_files
