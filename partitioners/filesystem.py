import os

from partitioner_interface import Partitioner


class FileSystemPartitioner(Partitioner):
    def __init__(self, files_dir):
        self.files_dir = files_dir

    def partition(self, partition_policy, access_control_policy):
        # Each output bucket contains the file paths of the files in that bucket
        file_buckets = dict()
        for root, _, files in os.walk(self.files_dir):
            for file in files:
                file_path = os.path.join(root, file)
                principal_id = access_control_policy(file_path)
                file_buckets.setdefault(partition_policy(principal_id), []).append(file_path)
        return file_buckets