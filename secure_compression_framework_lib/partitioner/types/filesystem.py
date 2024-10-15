import os
from pathlib import Path

from secure_compression_framework_lib.partitioner.partitioner import Partitioner


class FileSystemPartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for a directory containing files to be partitioned."""

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> dict[str, list[Path]]:
        file_buckets: dict[str, list[Path]] = {}
        for root, _, files in os.walk(self._get_data()):
            for file in files:
                file_path = os.path.join(root, file)
                principal_id = self.access_control_policy(Path(file_path))
                file_buckets.setdefault(self.partition_policy(principal_id), []).append(Path(file_path))
        return file_buckets
