from collections.abc import Callable
from pathlib import Path

from secure_compression_framework_lib.multi_stream.compress import (
    MSCompressor,
    MSDecompressor,
    ZlibCompressionStream,
    ZlibDecompressionStream,
)
from secure_compression_framework_lib.partitioner.access_control import Principal, XMLDataUnit, basic_partition_policy
from secure_compression_framework_lib.partitioner.types.xml_advanced import XmlAdvancedPartitioner


def compress_xml_advanced_by_element(
    xml_file: Path, access_control_policy: Callable[[XMLDataUnit], Principal]
) -> bytes:
    """Implements an XML compression scenario where the principal is encoded as an XML element.

    The application provides a Python function that extracts the principal from an XMLDataUnit (element + context) for
    use as the access control policy

    Args:
    ----
        xml_file: The XML file to be compressed
        access_control_policy: Access control policy provided by application

    Returns:
    -------
        Bytes of the safely compressed XML

    """
    partitioner = XmlAdvancedPartitioner(xml_file, access_control_policy, basic_partition_policy)
    bucketed_data = partitioner.partition()
    msc = MSCompressor(ZlibCompressionStream)
    for bucket, data in bucketed_data:
        msc.compress(bucket, data)
    return msc.finish()


def decompress_xml_advanced_by_element(ms_compressed_data: bytes) -> bytes:
    msd = MSDecompressor(ZlibDecompressionStream)
    msd.decompress(ms_compressed_data)
    return msd.finish()
