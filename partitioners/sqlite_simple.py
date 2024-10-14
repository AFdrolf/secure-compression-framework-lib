from partitioner_interface import Partitioner

class SQLiteSimplePartitioner(Partitioner):
    def __init__(self, db_path):
        self.db_path = db_path
    
    def partition(self, partition_policy, access_control_policy):
        pass