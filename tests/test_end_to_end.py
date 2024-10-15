import os

import pytest

from secure_compression_framework_lib.end_to_end.dedup_files import dedup_files_by_name


@pytest.fixture
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
