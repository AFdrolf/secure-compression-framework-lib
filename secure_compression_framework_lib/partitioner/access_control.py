"""Implements access control functionality for use by partitioner."""

from collections.abc import Callable
from typing import Any


class Principal:
    """Generic Principal class.

    To be instantiated with a relevant set of attributes, potentially with a bridge to a database.

    Can be instantiated with a dictionary of attributes. Subsequent attributes can be set/updated with p[k] = v, for a
    principal object P.

    __null is a special attribute. A null Principal is used when the data unit is not associated with any Principal.
    This can be either because no Principal has a view on it, or because the data units can be nested and a data unit
    actually contains multiple sub data units that different Principals have separate view on.
    """

    def __init__(self, null=False, **attr):
        for k, v in attr.items():
            assert not isinstance(v, dict)  # Breaks hash
            setattr(self, k, v)
        self.__null = null

    def __setitem__(self, k, v):
        assert not isinstance(v, dict)  # Breaks hash
        self.k = v

    def __getitem__(self, k):
        return self.k

    def __repr__(self):
        return repr(sorted(self.__dict__.items()))

    def __hash__(self):
        return hash(self.__repr__())

    def __str__(self):
        return str(self.__hash__())

    @property
    def null(self):
        return self.__null


# An access control policy maps a data unit to a principal
AccessControlPolicy: Callable[[Any], Principal]


# A partition policy maps a principal to a bucket label
PartitionPolicy: Callable[[Principal], str]


def basic_partition_policy(p: Principal) -> str:
    """Partitions based on the principal itself."""
    return str(p)


def attribute_based_partition_policy(p: Principal, attr: str) -> str:
    """Partitions based on a given attribute of the principal e.g. is_contact."""
    return str(p.__getattribute__(attr))
