from partitioner_interface import Partitioner

class FileSystemPartitioner(Partitioner):
    def __init__(self, files_dir):
        self.files_dir = files_dir

    def partition(self, partition_policy, access_control_policy):
        file_paths = []
        for root, _, files in os.walk(files_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_paths.append(file_path)

        classes = partition_policy(file_paths, principals)