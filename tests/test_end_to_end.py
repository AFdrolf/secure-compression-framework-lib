import os
from pathlib import Path
from xml.etree import ElementTree

import pytest

from secure_compression_framework_lib.end_to_end.compress_xml_advanced import (
    compress_xml_advanced_by_element,
    decompress_xml_advanced_by_element,
)
from secure_compression_framework_lib.end_to_end.dedup_files import dedup_files_by_name
from secure_compression_framework_lib.multi_stream.compress import ZlibCompressionStream
from tests.test_partitioner_xml_advanced import example_extract_principal_from_xml


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
    for child1, child2 in zip(tree1, tree2):
        if not trees_equivalent(child1, child2):
            return False
    return True


def test_compress_xml_advanced_basic():
    path = Path(__file__).parent / "examples/books.xml"
    et_before = ElementTree.parse(path).getroot()

    partition_compressed_bytes, stream_switch = compress_xml_advanced_by_element(
        path, example_extract_principal_from_xml
    )
    partition_decompressed_bytes = decompress_xml_advanced_by_element(partition_compressed_bytes, stream_switch)
    et_after = ElementTree.fromstring(partition_decompressed_bytes)

    cs = ZlibCompressionStream()
    cs.compress(path.read_bytes())
    regular_compressed_bytes = cs.finish()

    # Adversary inserted a final book that without partitioning gets compressed with the previous book
    assert len(partition_compressed_bytes) > len(regular_compressed_bytes)

    assert trees_equivalent(et_before, et_after)
