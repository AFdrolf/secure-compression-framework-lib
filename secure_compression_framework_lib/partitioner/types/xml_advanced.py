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

    element: ElementTree.Element
    context: list[ElementTree.Element]  # List of parent elements, starting from root


def generate_start_tag(element: ElementTree.Element) -> str:
    """Generate the start tag for an XML element."""
    tag = f"<{element.tag}"
    for key, value in element.attrib.items():
        tag += f' {key}="{value}"'
    tag += f">{element.text}"
    return tag


def generate_end_tag(element: ElementTree.Element) -> str:
    """Generate the end tag for an XML element."""
    return f"</{element.tag}>\n"


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
        target_event = None
        for event, element in ElementTree.iterparse(self._get_data(), events=["start", "end"]):
            if target_event and (event, element) != target_event:
                continue
            else:
                target_event = None
            data_unit = XMLDataUnit(element, parent_stack)
            principal = self.access_control_policy(data_unit)
            bucket = self.partition_policy(principal)
            if event == "start":
                if principal.null:
                    # This indicates that the element has child elements that map to different principals
                    # Add just the start tag to the null bucket and check children
                    parent_stack.append(element)
                    start_tag = generate_start_tag(element)
                    bucketed_data.append((bucket, start_tag.encode("utf-8")))
                else:
                    # If we have a non-null principal we can wait for the whole element to be parsed and bucket it
                    # Ignore events until we come to end of this element
                    target_event = ("end", element)
            elif event == "end":
                if principal.null:
                    # We already bucketed the start tag, just add end tag to null bucket
                    parent_stack.pop()
                    end_tag = generate_end_tag(element)
                    bucketed_data.append((bucket, end_tag.encode("utf-8")))
                else:
                    # Now that we have the whole element, bucket it
                    bucketed_data.append((bucket, ElementTree.tostring(element)))
        return bucketed_data
