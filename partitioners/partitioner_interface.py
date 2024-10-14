class Partioner:
    def __init__(self, data_to_partition):
        # data_to_partition may be bytes (after some other function serializes storage), a string representing a location in storage, etc
        self.data_to_partition = data_to_partition
    
    def partition(self, partition_policy, access_control_policy):
        # Additional args for a comparison function? E.g., to specify which hash to use?

        # access_control_policy(data_unit) -> principal label
        # partition_policy(user) -> bucket for this user
        raise NotImplementedError