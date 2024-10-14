class Partioner:
    def __init__(self, data_to_partition):
        # data_to_partition may be bytes (after some other function serializes storage), a string representing a location in storage, etc
        self.data_to_partition = data_to_partition
    
    def partition(self, partition_policy, access_control_policy) -> list[list[object]]:
        return [l for l in self.labeled_data.values()]