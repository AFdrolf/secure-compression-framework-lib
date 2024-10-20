from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

from secure_compression_framework_lib.partitioner.access_control import Principal
from secure_compression_framework_lib.partitioner.partitioner import Partitioner


@dataclass
class XMLDataUnit:
    """An XMLDataUnit is the unit which is mapped to a Principal.

    The unit we actually want to map to a Principal is an XML element, but to do this mapping we need some context for
    the element. To see why this is necessary imagine the case where an XML file has nested elements that both have the
    name "user".
    """

    context: list[ElementTree.Element]  # List of parent elements, starting from root

    @property
    def element(self) -> ElementTree.Element:
        return self.context[-1]


def generate_start_tag(element: ElementTree.Element) -> str:
    """Generate the start tag for an XML element."""
    tag = f"<{element.tag}"
    for key, value in element.attrib.items():
        tag += f' {key}="{value}"'
    tag += f">{element.text}"
    return tag


def generate_end_tag(element: ElementTree.Element) -> str:
    """Generate the end tag for an XML element."""
    if element.tail:
        tail = element.tail
    else:
        tail = "\n"
    return f"</{element.tag}>{tail}"


class XmlAdvancedPartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for a file containing XML to be partitioned.

    Attributes:
    ----------
        data: A Path object for an XML file
        access_control_policy: Maps XMLDataUnit objects to Principals (Callable[[XMLDataUnit], Principal])

    Todo:
    ----
        We can provide a helper function that accepts a xpath-like string to generate an access control function

    """

    def _get_data(self) -> Path:
        return self.data

    @staticmethod
    def access_control_from_xpath(self, xpath: str) -> Callable[[XMLDataUnit], Principal]:
        pass

    def partition(self) -> list[tuple[str, bytes]]:
        bucketed_data = []
        parent_stack = []
        for event, element in ElementTree.iterparse(self._get_data(), events=["start", "end"]):
            if event == "start":
                tag = generate_start_tag(element)
                parent_stack.append(element)
                data_unit = XMLDataUnit(parent_stack)
            else:
                tag = generate_end_tag(element)
                parent_stack.pop()
                data_unit = XMLDataUnit([*parent_stack, element])

            principal = self.access_control_policy(data_unit)
            bucket = self.partition_policy(principal)

            if not bucketed_data or bucket != bucketed_data[-1][0]:
                # New bucket
                bucketed_data.append((bucket, bytearray(tag.encode("utf-8"))))
            else:
                bucketed_data[-1][1].extend(tag.encode("utf-8"))

        return bucketed_data
