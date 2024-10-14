import xml.etree.ElementTree as ET

from secure_compression_framework_lib.partitioner.partitioner import Partitioner


# TODO: we may need to make this code more efficient to deal with large databases.

class SQLiteSimplePartitioner(Partitioner):
    def __init__(self, db_path):
        self.db_path = db_path
    
    def partition(self, partition_policy, access_control_policy):
        db_buckets = dict()


        tree = ET.parse(self.db_path)
        root = tree.getroot()

        # First, iterate through all XML elements
        for xml_element in root.findall('.//*'):
            # TODO: pre-process element to make it compatible with access control policy (maybe just get the primary key)?
            principal_id = access_control_policy(xml_element)
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
            bucket_tree.write(db_bucket_path, encoding='utf-8', xml_declaration=True)

        return




