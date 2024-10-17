import xml.etree.ElementTree as ET

import sys
sys.path.append(sys.path[0] + '/../../..')
from secure_compression_framework_lib.partitioner.partitioner import Partitioner

# TODO: we may need to make this code more efficient to deal with large databases.


class XMLSimplePartitioner(Partitioner):
    def __init__(self, db_path):
        self.db_path = db_path

    def partition(self, partition_policy, access_control_policy):
        db_buckets = {}

        tree = ET.parse(self.db_path)
        root = tree.getroot()

        # First, iterate through all XML elements
        for xml_element in root.findall(".//*"):
            # TODO: pre-process element to make it compatible with access control policy (maybe just get the primary key)?
            principal_id = access_control_policy(xml_element)
            if principal_id == None:
                continue
            db_bucket_id = partition_policy(principal_id)

            # Create empty XML file if it does not exist yet
            if db_bucket_id not in db_buckets:
                db_bucket_root = ET.Element(db_bucket_id)
                db_buckets[db_bucket_id] = db_bucket_root
            else:
                db_bucket_root = db_buckets[db_bucket_id]

            # Then, add element to its respective bucket DB
            db_bucket_root.append(xml_element)

        for db_bucket_id, db_bucket_root in db_buckets.items():
            bucket_tree = ET.ElementTree(db_bucket_root)
            # TODO: generalize so that user can specify name
            db_bucket_path = db_bucket_id + self.db_path
            bucket_tree.write(db_bucket_path, encoding="utf-8", xml_declaration=True)

# For testing, delete later
import os

def create_messages_xml(file_name):
    """Create an XML file with a messages structure."""
    # Create the root element
    root = ET.Element("messages")
    
    # Create an ElementTree object
    tree = ET.ElementTree(root)

    # Write the XML file
    with open(file_name, 'wb') as xml_file:
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
    with open(file_name, 'wb') as xml_file:
        tree.write(xml_file)


if __name__ == "__main__":
    import os
    import glob

    file_type = "*.xml"

    for file_path in glob.glob(file_type):
        os.remove(file_path)

    db_name = 'messages.xml'
    create_messages_xml(db_name)

    # Params: db_name, gid, from_me, content
    add_message_to_xml(db_name, 1, 1, '1Hello, World!')
    add_message_to_xml(db_name, 1, 1, '2Hello, World!')
    add_message_to_xml(db_name, 2, 1, '3Hello, World!')
    add_message_to_xml(db_name, 7, 1, '4Hello, World!')
    add_message_to_xml(db_name, 7, 1, '5Hello, World!')
    add_message_to_xml(db_name, 7, 1, '7Hello, World!')

    partitioner = XMLSimplePartitioner(db_name)

    def access(element):
        if element.tag == "message":
            return element.find('gid').text
        else:
            return "metadata"
    
    def partition_policy(id):
        return id

    partitioner.partition(partition_policy, access)