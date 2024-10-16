from pathlib import Path
from xml.etree import ElementTree

from secure_compression_framework_lib.partitioner.access_control import Principal

from secure_compression_framework_lib.partitioner.access_control import basic_partition_policy
from secure_compression_framework_lib.partitioner.types.xml_advanced import XmlAdvancedPartitioner, XMLDataUnit


def example_extract_principal_from_xml(xml_du: XMLDataUnit) -> Principal:
    """Example access control policy function.

    Assumes that principals can have views on books and the principal with a view on the book is encoded as an XML
    element catalog/book/author which has value "principal_last_name, principal_first_name"
    """
    if [e.tag for e in xml_du.context] == ["catalog"] and xml_du.element.tag == "book":
        # Context is correct to extract principal
        principal_element = xml_du.element.find("author")
        principal_value = principal_element.text.split(", ")
        principal_first_name = principal_value[1]
        principal_last_name = principal_value[0]
        return Principal(first_name=principal_first_name, last_name=principal_last_name)
    else:
        # Context is incorrect so return a null principal
        return Principal(null=True)


def test_partitioner_xml_advanced():
    path = Path(__file__).parent / "examples/books.xml"
    partitioner = XmlAdvancedPartitioner(
        path,
        example_extract_principal_from_xml,
        basic_partition_policy
    )
    out = partitioner.partition()
    assert len(out) == 15
    assert out[0] == (str(Principal(null=True)), "<catalog>\n   ".encode("utf-8"))
    assert out[-1] == (str(Principal(null=True)), "</catalog>\n".encode("utf-8"))
    book1_element = ElementTree.parse(path).getroot().find(".//book")
    assert out[1] == (str(Principal(first_name="Matthew", last_name="Gambardella")), ElementTree.tostring(book1_element))