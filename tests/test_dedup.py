"""Tests for multi stream deduplication."""

import hashlib
import os

import pytest

from secure_compression_framework_lib.multi_stream.dedup import checksum_comparison_function, dedup


@pytest.fixture(params=[10, 100])
def scratch_dir(request, tmpdir):
    r1 = os.urandom(100)
    r2 = os.urandom(100)
    for i in range(request.param):
        p = tmpdir.join(f"test_file_{i})")
        if i % 2 == 0:
            p.write(r1, mode="wb")
        else:
            p.write(r2, mode="wb")
    return tmpdir


def test_dedup_basic(scratch_dir):
    h = hashlib.sha256
    chunk_size = 65536
    comparison_function = lambda file_path: checksum_comparison_function(file_path, h, chunk_size)
    deduped_files = dedup(comparison_function, scratch_dir.listdir())
    assert len(deduped_files) == 2
