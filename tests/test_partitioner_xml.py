from pathlib import Path
from xml.etree import ElementTree

from injection_attacks_mitigation_framework.partitioner.access_control import (
    Principal,
    XMLDataUnit,
    basic_partition_policy,
)
from injection_attacks_mitigation_framework.partitioner.types.xml_advanced import XmlAdvancedPartitioner
from injection_attacks_mitigation_framework.partitioner.types.xml_simple import XMLSimplePartitioner


def example_author_as_principal_books_xml(xml_du: XMLDataUnit) -> Principal:
    """Example access control policy function.

    Assumes that principals can have views on books and the principal with a view on the book is encoded as an XML
    element catalog/book/author which has value "principal_last_name, principal_first_name"
    """
    if [e.tag for e in xml_du.context[:2]] == ["catalog", "book"]:
        # Context is correct to extract principal
        principal_element = xml_du.context[1].find("author")
        principal_value = principal_element.text.split(", ")
        principal_first_name = principal_value[1]
        principal_last_name = principal_value[0]
        return Principal(first_name=principal_first_name, last_name=principal_last_name)
    else:
        # Context is incorrect so return a null principal
        return Principal(null=True)


def test_partitioner_xml_advanced_author_as_principal():
    path = Path(__file__).parent / "example_data/books.xml"
    partitioner = XmlAdvancedPartitioner(path, example_author_as_principal_books_xml, basic_partition_policy)
    out = partitioner.partition()
    assert len(out) == 12
    assert out[0] == (str(Principal(null=True)), b"<catalog>\n   ")
    assert out[-1] == (str(Principal(null=True)), b"</catalog>\n")
    book_elements = ElementTree.parse(path).getroot().findall(".//book")
    assert out[1] == (
        str(Principal(first_name="Matthew", last_name="Gambardella")),
        ElementTree.tostring(book_elements[0]),
    )
    assert out[3] == (
        str(Principal(first_name="Eva", last_name="Corets")),
        b"".join([ElementTree.tostring(e) for e in book_elements[2:5]]),
    )


def example_author_as_principal_title_separate_books_xml(xml_du: XMLDataUnit) -> Principal:
    """Example access control policy function.

    Assumes that principals can have views on books and the principal with a view on the book is encoded as an XML
    element catalog/book/author which has value "principal_last_name, principal_first_name. The title element of each
    book is under the view of a separate principal whose name is the title/"
    """
    if [e.tag for e in xml_du.context[:3]] == ["catalog", "book", "title"]:
        # Title is view of a different principal
        principal_element = xml_du.context[2]
        principal_value = principal_element.text
        return Principal(name=principal_value)
    elif [e.tag for e in xml_du.context[:2]] == ["catalog", "book"]:
        # Context is correct to extract principal
        principal_element = xml_du.context[1].find("author")
        principal_value = principal_element.text.split(", ")
        principal_first_name = principal_value[1]
        principal_last_name = principal_value[0]
        return Principal(first_name=principal_first_name, last_name=principal_last_name)
    else:
        # Context is incorrect so return a null principal
        return Principal(null=True)


def test_partitioner_xml_advanced_author_as_principal_title_separate():
    path = Path(__file__).parent / "example_data/books.xml"
    partitioner = XmlAdvancedPartitioner(
        path, example_author_as_principal_title_separate_books_xml, basic_partition_policy
    )
    out = partitioner.partition()
    print(out)
    assert len(out) == 38
    book_elements = ElementTree.parse(path).getroot().findall(".//book")
    title_elements = ElementTree.parse(path).getroot().findall(".//title")
    assert out[1] == (
        str(Principal(first_name="Matthew", last_name="Gambardella")),
        ElementTree.tostring(book_elements[0]).split(b"<title")[0],
    )
    assert out[2] == (
        str(Principal(name="XML Developer's Guide")),
        ElementTree.tostring(title_elements[0]),
    )
    assert out[7] == (
        str(Principal(first_name="Eva", last_name="Corets")),
        ElementTree.tostring(book_elements[2]).split(b"<title")[0],
    )
    assert out[8] == (
        str(Principal(name="Maeve Ascendant")),
        ElementTree.tostring(title_elements[2]),
    )
    book3_post_title = ElementTree.tostring(book_elements[2]).split(b"title>\n      ")[-1]
    book4_pre_title = ElementTree.tostring(book_elements[3]).split(b"<title")[0]
    assert out[9] == (
        str(Principal(first_name="Eva", last_name="Corets")),
        book3_post_title + book4_pre_title,
    )


def example_group_uuid_as_principal_keepass_sample_xml(xml_du: XMLDataUnit) -> Principal:
    """Example access control policy function.

    Assumes that principals can have views on keepass entries in a certain group and the principal with a view on the
    groups entries is encoded as an XML element KeePassFile/Root/Group/[Group]... (groups can be nested) where we can
    use the UUID element under group to identify the principal uniquely.
    """
    # Find the most immediate Group ancestor in tree
    for element in reversed(xml_du.context):
        if element.tag == "Group":
            uuid_element = element.find("UUID")
            if uuid_element is not None:
                return Principal(uuid=uuid_element.text)
    # Not under a Group
    return Principal(null=True)


def test_partitioner_xml_advanced_keepass_group_as_principal():
    path = Path(__file__).parent / "example_data/keepass_sample.xml"
    partitioner = XmlAdvancedPartitioner(
        path, example_group_uuid_as_principal_keepass_sample_xml, basic_partition_policy
    )
    out = partitioner.partition()
    assert len(out) == 6


def test_partitioner_xml_simple_author_as_principal():
    path = Path(__file__).parent / "example_data/books.xml"
    book_elements = ElementTree.parse(path).getroot().findall(".//book")
    partitioner = XMLSimplePartitioner(path, example_author_as_principal_books_xml, basic_partition_policy)
    out = partitioner.partition()
    assert len(out) == 11
    assert out[str(Principal(null=True))] == b"<catalog>\n   </catalog>"
    book_elements[0].set("bucketed", "true")
    assert (
        out[str(Principal(first_name="Matthew", last_name="Gambardella"))]
        == b"<catalog>\n   " + ElementTree.tostring(book_elements[0])[:-3] + b"</catalog>"
    )


def test_partitioner_xml_simple_keepass_group_as_principal():
    path = Path(__file__).parent / "example_data/keepass_sample.xml"
    partitioner = XMLSimplePartitioner(path, example_group_uuid_as_principal_keepass_sample_xml, basic_partition_policy)
    out = partitioner.partition()
    assert len(out) == 4
