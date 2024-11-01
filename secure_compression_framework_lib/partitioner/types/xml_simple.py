import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

from secure_compression_framework_lib.partitioner.access_control import Principal, XMLDataUnit

sys.path.append(sys.path[0] + "/../../..")
from secure_compression_framework_lib.partitioner.partitioner import Partitioner

# TODO: we may need to make this code more efficient to deal with large databases.


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


# For testing, delete later
import os


def create_messages_xml(file_name):
    """Create an XML file with a messages structure."""
    # Create the root element
    root = ET.Element("messages")

    # Create an ElementTree object
    tree = ET.ElementTree(root)

    # Write the XML file
    with open(file_name, "wb") as xml_file:
        tree.write(xml_file)


def add_message_to_xml(file_name, gid, from_me, content):
    """Add a message entry to the XML file."""
    # Load existing XML or create a new one if it doesn't exist
    if os.path.exists(file_name):
        tree = ET.parse(file_name)
        root = tree.getroot()
    else:
        root = ET.Element("messages")

    # Create a new message element
    message = ET.Element("message")

    # Generate a new id based on the current number of messages
    message_id = len(root.findall("message")) + 1

    # Set the attributes for the message
    id_elem = ET.SubElement(message, "id")
    id_elem.text = str(message_id)

    gid_elem = ET.SubElement(message, "gid")
    gid_elem.text = str(gid)

    from_me_elem = ET.SubElement(message, "from_me")
    from_me_elem.text = str(from_me)

    content_elem = ET.SubElement(message, "content")
    content_elem.text = content

    # Append the new message to the root element
    root.append(message)

    # Write the updated tree back to the file
    tree = ET.ElementTree(root)
    with open(file_name, "wb") as xml_file:
        tree.write(xml_file)


if __name__ == "__main__":
    import glob
    import os

    file_type = "*.xml"

    for file_path in glob.glob(file_type):
        os.remove(file_path)
    exit()
    db_name = "messages.xml"
    create_messages_xml(db_name)

    # Params: db_name, gid, from_me, content
    add_message_to_xml(db_name, 1, 1, "1Hello, World!")
    add_message_to_xml(db_name, 1, 1, "2Hello, World!")
    add_message_to_xml(db_name, 2, 1, "3Hello, World!")
    add_message_to_xml(db_name, 7, 1, "4Hello, World!")
    add_message_to_xml(db_name, 7, 1, "5Hello, World!")
    add_message_to_xml(db_name, 7, 1, "7Hello, World!")

    def access(element):
        if element.tag == "message":
            return element.find("gid").text
        else:
            return "metadata"

    def partition_policy(id):
        return id

    partitioner = XMLSimplePartitioner(Path(db_name), access, partition_policy)

    print(partitioner.partition())
