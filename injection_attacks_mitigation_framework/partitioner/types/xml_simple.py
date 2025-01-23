import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

from injection_attacks_mitigation_framework.partitioner.access_control import Principal, XMLDataUnit

sys.path.append(sys.path[0] + "/../../..")
from injection_attacks_mitigation_framework.partitioner.partitioner import Partitioner


class XMLSimplePartitioner(Partitioner):
    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> dict[str, bytes]:
        db_buckets = defaultdict(list)
        parent_stack = []
        to_remove = defaultdict(list)

        # First, iterate through all XML elements
        for event, element in ET.iterparse(self._get_data(), events=["start", "end"]):
            if event == "start":
                parent_stack.append(element)
                data_unit = XMLDataUnit(parent_stack)
            else:
                parent_stack.pop()
                if element in to_remove:
                    for e in to_remove[element]:
                        element.remove(e)
                continue

            principal = self.access_control_policy(data_unit)
            db_bucket_id = self.partition_policy(principal)

            if len(parent_stack) > 1:
                parent_bucket_id = self.partition_policy(self.access_control_policy(XMLDataUnit(parent_stack[:-1])))
            else:
                # This should only execute for the root element
                db_buckets[db_bucket_id].append((element, []))
                continue

            if db_bucket_id != parent_bucket_id:
                # Remove current element from parent element
                to_remove[parent_stack[-2]].append(element)

                # Store path to element along with element so that we can merge element with metadata
                db_buckets[db_bucket_id].append((element, [e.tag for e in parent_stack[:-1]]))
            else:
                continue

        # Create bucketed trees by merging elements in each bucket with metadata
        tree_buckets = {}

        for k, element_list in db_buckets.items():
            for e, path in sorted(element_list, key=lambda x: len(x[1])):
                if not path:
                    # Handle root element
                    tree_buckets[k] = e
                    continue
                if k not in tree_buckets:
                    tree_buckets[k] = ET.Element(path[0])
                target_element = tree_buckets[k]
                for p in path[1:]:
                    if tree_buckets[k].find(p):
                        target_element = target_element.find(p)
                    else:
                        target_element = ET.SubElement(target_element, p)

                e.set("bucketed", "true")  # Signals that an element is bucketed, for use when recombining
                target_element.append(e)

            ET.indent(tree_buckets[k], space="   ")

        return {k: ET.tostring(v) for k, v in tree_buckets.items()}
