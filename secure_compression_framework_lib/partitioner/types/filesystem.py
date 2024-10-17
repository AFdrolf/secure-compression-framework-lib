import os
from pathlib import Path

from secure_compression_framework_lib.partitioner.partitioner import Partitioner


class FileSystemPartitioner(Partitioner):
    """Implements partitioner for files.

    Attributes:
        data: A Path object for a directory containing files to be partitioned.
        access_control_policy: Maps Path objects to Principals (Callable[[Path], Principal])
        partition_policy: Maps Principals to buckets
        """

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> list[tuple[str, Path]]:
        file_buckets = []
        for root, _, files in os.walk(self._get_data()):
            for file in files:
                file_path = os.path.join(root, file)
                principal_id = self.access_control_policy(Path(file_path))
                file_buckets.append((self.partition_policy(principal_id), Path(file_path)))
        return file_buckets
