"""Base class for partitioners."""

from collections.abc import Callable
from typing import Any

from injection_attacks_mitigation_framework.partitioner.access_control import Principal


class Partitioner:
    """Base class for partitioners.

    The one requirement for a partitioner is that it implements a partition function that is data format dependent,
    which uses the policy attributes to partition the data into buckets which can be passed to downstream functions
    individually to prevent cross-user data interaction.

    Attributes
    ----------
        data: The data to be partitioned.
        access_control_policy: Maps data units to Principals
        partition_policy: Maps Principals to buckets

    """

    def __init__(
        self, data: Any, access_control_policy: Callable[[Any], Principal], partition_policy: Callable[[Principal], str]
    ) -> None:
        self.data = data
        self.access_control_policy = access_control_policy
        self.partition_policy = partition_policy

    @property
    def _get_data(self) -> Any:
        """Useful in child when data type is known to allow type checking."""
        raise NotImplementedError

    def partition(self) -> list[tuple[str, Any]]:
        """To be implemented by child to handle a specific data format.

        This returns a list of tuples rather than a dict because for e.g. compression if one file is split into multiple
        buckets we need to maintain the ordering
        """
        raise NotImplementedError
