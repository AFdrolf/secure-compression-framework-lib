import os
from pathlib import Path
from xml.etree import ElementTree

import pytest

from secure_compression_framework_lib.end_to_end.compress_sqlite_advanced import compress_sqlite_advanced, \
    decompress_sqlite_advanced
from secure_compression_framework_lib.end_to_end.compress_xml_advanced import (
    compress_xml_advanced_by_element,
    decompress_xml_advanced_by_element,
)
from secure_compression_framework_lib.end_to_end.compress_xml_simple import compress_xml_simple, decompress_xml_simple
from secure_compression_framework_lib.end_to_end.dedup_files import dedup_files_by_name
from secure_compression_framework_lib.multi_stream.compress import ZlibCompressionStream
from secure_compression_framework_lib.partitioner.access_control import basic_partition_policy, \
    generate_attribute_based_partition_policy
from tests.test_partitioner_sqlite import gid_as_principal_access_control_policy
from tests.test_partitioner_xml import (
    example_author_as_principal_books_xml,
    example_group_uuid_as_principal_keepass_sample_xml,
)


@pytest.fixture()
def scratch_dir(tmpdir):
    duplicated_attachment = os.urandom(1024)
    tmpdir.mkdir("attachments_1")
    tmpdir.mkdir("attachments_2")
    a1 = tmpdir.join("attachments_1")
    a2 = tmpdir.join("attachments_2")

    # Here _d suffix used for clarity to indicate attachment has a duplicate
    a1.join("bob_attachment_1_d").write(duplicated_attachment, mode="wb")  # Should be deduplicated
    a1.join("bob_attachment_2").write(os.urandom(1024), mode="wb")
    a2.join("bob_attachment_3_d").write(duplicated_attachment, mode="wb")  # Should be deduplicated
    a1.join("alice_attachment_1").write(os.urandom(1024), mode="wb")
    a2.join("alice_attachment_2_d").write(duplicated_attachment, mode="wb")  # Should not be deduplicated

    return tmpdir


def test_dedup_files_by_name_basic(scratch_dir):
    deduped_files = dedup_files_by_name(scratch_dir)
    assert len(deduped_files) == 4
    deduped_filenames = [p.name for p in deduped_files]
    assert "bob_attachment_2" in deduped_filenames
    assert ("bob_attachment_1_d" in deduped_filenames) ^ ("bob_attachment_3_d" in deduped_filenames)
    assert "alice_attachment_1" in deduped_filenames
    assert "alice_attachment_2_d" in deduped_filenames


# TODO (andres): end-to-end tests for sqlite, xml simple.


def trees_equivalent(tree1, tree2):
    """Test if two ElementTree trees are equivalent."""
    if tree1.tag != tree2.tag:
        return False
    if tree1.attrib != tree2.attrib:
        return False
    if len(tree1) != len(tree2):
        return False

    alphabetical_tree1 = sorted([c for c in tree1], key=lambda c: c.tag)
    alphabetical_tree2 = sorted([c for c in tree2], key=lambda c: c.tag)
    for child1, child2 in zip(alphabetical_tree1, alphabetical_tree2):
        if not trees_equivalent(child1, child2):
            return False
    return True


@pytest.mark.parametrize(
    "file,policy",
    [
        ("books.xml", example_author_as_principal_books_xml),
        ("keepass_sample.xml", example_group_uuid_as_principal_keepass_sample_xml),
    ],
)
def test_compress_xml_advanced_basic(file, policy):
    path = Path(__file__).parent / f"example_data/{file}"
    et_before = ElementTree.parse(path).getroot()

    partition_compressed_bytes = compress_xml_advanced_by_element(path, policy)
    partition_decompressed_bytes = decompress_xml_advanced_by_element(partition_compressed_bytes)
    et_after = ElementTree.fromstring(partition_decompressed_bytes)

    cs = ZlibCompressionStream()
    cs.compress(path.read_bytes())
    regular_compressed_bytes = cs.finish()

    assert len(partition_compressed_bytes) > len(regular_compressed_bytes)

    assert trees_equivalent(et_before, et_after)


@pytest.mark.parametrize(
    "file,policy",
    [
        ("books.xml", example_author_as_principal_books_xml),
        ("keepass_sample.xml", example_group_uuid_as_principal_keepass_sample_xml),
    ],
)
def test_compress_xml_simple_basic(file, policy):
    path = Path(__file__).parent / f"example_data/{file}"
    et_before = ElementTree.parse(path).getroot()

    partition_compressed_bytes = compress_xml_simple(path, policy, basic_partition_policy)
    partition_decompressed_bytes = decompress_xml_simple(partition_compressed_bytes)
    et_after = ElementTree.fromstring(partition_decompressed_bytes)

    cs = ZlibCompressionStream()
    cs.compress(path.read_bytes())
    regular_compressed_bytes = cs.finish()

    assert sum(len(x) for x in partition_compressed_bytes) > len(regular_compressed_bytes)

    assert trees_equivalent(et_before, et_after)


def test_compress_sql_advanced_basic(scratch_dir):
    path = Path(__file__).parent / "example_data/whatsapp_sample.db"

    partition_compressed_bytes = compress_sqlite_advanced(path, gid_as_principal_access_control_policy, generate_attribute_based_partition_policy("gid"))
    partition_decompressed_bytes = decompress_sqlite_advanced(partition_compressed_bytes)

    cs = ZlibCompressionStream()
    cs.compress(path.read_bytes())
    regular_compressed_bytes = cs.finish()

    assert len(partition_compressed_bytes) > len(regular_compressed_bytes)
    assert path.read_bytes() == partition_decompressed_bytes
