"""Implements access control functionality for use by partitioner."""

from collections import defaultdict
from collections.abc import Callable


class Principal:
    """Generic Principal class.

    To be instantiated with a relevant set of attributes, potentially with a bridge to a database.

    Can be instantiated with a dictionary of attributes. Subsequent attributes can be set/updated with p[k] = v, for a
    principal object P.
    """

    def __init__(self, **attr):
        for k, v in attr.items():
            assert not isinstance(v, dict)  # Breaks hash
            setattr(self, k, v)

    def __setitem__(self, k, v):
        assert not isinstance(v, dict)
        self.k = v

    def __getitem__(self, k):
        return self.k

    def __hash__(self):
        return hash(self.__dict__)

    def __repr__(self):
        return self.__hash__()


class AccessPolicy:
    """Maps object to Principal.

    Basic POC implementation is a dict. But we would like this to be a blackbox function provided by application?
    """

    def __init__(self, views: dict[object, Principal]) -> None:
        self.views = defaultdict(Principal, views)

    def map(self, o: object) -> Principal:
        return self.views[o]


# A partition policy maps a principal to a bucket label
PartitionPolicy: Callable[[Principal], str]


def attribute_based_partition_policy(p: Principal, attr: str) -> str:
    """Partitions based on a given attribute e.g. is_contact."""
    return str(p.__getattribute__(attr))
