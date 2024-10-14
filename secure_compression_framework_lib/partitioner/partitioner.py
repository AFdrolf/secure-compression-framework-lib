from typing import Any


class Partitioner:
    def __init__(self, data: Any) -> None:
        # data_to_partition may be bytes (after some other function serializes storage), a string representing a location in storage, etc
        self.data = data

    def partition(self, partition_policy, access_control_policy):
        # Additional args for a comparison function? E.g., to specify which hash to use?

        # access_control_policy(data_unit) -> principal label
        # partition_policy(user) -> bucket for this user
        # Sam: Think maybe policies should be attributes of class
        raise NotImplementedError
