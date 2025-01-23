import copy
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable

from injection_attacks_mitigation_framework.multi_stream.compress import (
    MSDecompressor,
    ZlibCompressionStream,
    ZlibDecompressionStream,
)
from injection_attacks_mitigation_framework.partitioner.access_control import Principal, XMLDataUnit
from injection_attacks_mitigation_framework.partitioner.types.xml_simple import XMLSimplePartitioner


def compress_xml_simple(
    xml_file: Path,
    access_control_policy: Callable[[XMLDataUnit], Principal],
    partition_policy: Callable[[Principal], str],
) -> list[bytes]:
    partitioner = XMLSimplePartitioner(xml_file, access_control_policy, partition_policy)
    bucketed_data = partitioner.partition()
    compressed_data = []
    for bucket, data in bucketed_data.items():
        c = ZlibCompressionStream()
        c.compress(data)
        compressed_data.append(c.finish())
    return compressed_data


def _combine_elements(e1: ET.Element, e2: ET.Element) -> None:
    for e in e2:
        if e.tag in [x.tag for x in e1] and not bool(e.get("bucketed")):
            # e1 already has element with that tag, descend into that
            _combine_elements(e1.find(e.tag), e)
        else:
            # e1 does not have element with that tag, insert it
            e1.append(e)
            # if marked with bucketed attr remove it
            if "bucketed" in e.attrib:
                del e.attrib["bucketed"]


def _merge_etrees(trees: list[ET.Element]) -> ET.Element:
    base_tree = copy.deepcopy(trees[0])
    for tree in trees[1:]:
        _combine_elements(base_tree, tree)
    return base_tree


def decompress_xml_simple(compressed_data: list[bytes]) -> bytes:
    decompressed_trees = []
    for tree in compressed_data:
        d = ZlibDecompressionStream()
        d.decompress(tree)
        decompressed_trees.append(ET.fromstring(d.finish().decode("utf-8")))

    tree = _merge_etrees(decompressed_trees)
    ET.indent(tree, space="   ")
    return ET.tostring(tree)
