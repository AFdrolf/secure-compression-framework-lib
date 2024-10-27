from collections import defaultdict
from dataclasses import dataclass
import os
from pathlib import Path
import sqlite3
import struct

from secure_compression_framework_lib.partitioner.partitioner import Partitioner

HEADER_SIZE_BYTES = 100
PAGE_SIZE_INFO_START_BYTE = 16
PAGE_SIZE_INFO_END_BYTE = 18
# Each start,end is inclusive
HEADER_INFO_POSITIONS = {
    "header_string_start": 0,
    "header_string_end": 15,
    "page_size_start": 16,
    "page_size_end": 17,
    "freelist_first_page_number_start": 32,
    "freelist_first_page_number_start": 35,
}
HEADER_STRING = "SQLite format 3\000"

PAGE_TYPES = {
    "table_leaf": 0x0d,
    "table_interior": 0x05,
    "index_leaf": 0x0a,
    "index_interior": 0x02,
}

@dataclass
class SQLiteDataUnit:
    """An SQLiteDataUnit is the unit which is mapped to a Principal.

    The unit we actually want to map to a Principal is a row in the database, but to do this mapping we need some context for
    the row (i.e., what table it belongs to)
    """

    row: tuple
    table_name: str

class SQLiteAdvancedPartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for the SQLite database file to be partitioned."""

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> list[Path]:
        self.buckets = defaultdict(bytes)

        con = sqlite3.connect(self._get_data())
        cur = con.cursor()

        db_size = self._get_data.stat().st_size
        with open(self._get_data(), "rb") as f:
            # First, check header, and find page size
            header = f.read(HEADER_SIZE_BYTES)
            if header[HEADER_INFO_POSITIONS["header_string_start"]:HEADER_INFO_POSITIONS["header_string_end"]+1].decode("ascii") != HEADER_STRING:
                raise ValueError("Input file is not encoded in SQLite's database file format.")
            page_size = struct.unpack("<H", header[HEADER_INFO_POSITIONS["page_size_start"]:HEADER_INFO_POSITIONS["page_size_end"]+1])[0]
            free_list_first_page = header[HEADER_INFO_POSITIONS["freelist_first_page_number_start"]:HEADER_INFO_POSITIONS["freelist_first_page_number_start"]+1]

            # We need to keep track of freelist and overflow pages, since there is no identifier for these
            # TODO: maybe we do not actually care about freelist pages? Also, fix: overflow page may technically appear before the parent page
            freelist_first_page = header[HEADER_INFO_POSITIONS["freelist_first_page_number_start"]:HEADER_INFO_POSITIONS["freelist_first_page_number_start"]+1]
            freelist_pages = [free_list_first_page]
            overflow_pages = []

            # Main loop: iterate through every page, determine its type, and handle as needed
            for page_number in range(1, db_size//page_size):
                page_start = page_number*page_size
                f.seek(page_start)
                page = f.read(page_size)
                page_type = page[0]

                if page_number in overflow_pages:
                    pass
                # Root page, containing the database header and schema information
                elif page_number == 1:
                    self.buckets["metadata"] += page
                # Locking page, which always contains all zeroes
                elif page_number == 2**30:
                    self.buckets["metadata"] += page
                # Either table interior, index interior, or index b-tree leaf page. These do not contain cell data
                elif page_type in PAGE_TYPES.values() and page_type != PAGE_TYPES["table_leaf"]:
                    self.buckets["metadata"] += page
                # Table b-tree leaf page
                elif page_type == PAGE_TYPES["table_leaf"]:
                    # TODO: find table name.
                    # We can do this heuristically by comparing sqlite_schema.sql against the payload of the row
                    # Or, do it _very_ slowly by iterating through all tables and seeing if the row is in the table
                    cell_rowid, record_data = self._parse_btree_leaf(page)

                    # For now, read the contents of the database by making a read-call to it. We can also parse this directly from record_data. TODO: check overflow page, if needed
                    table_name = self._find_table_name(cur, cell_rowid, record_data)
                    cur.execute("SELECT * FROM {table_name} WHERE rowid = ?", (cell_rowid,))
                    row = cur.fetchone()
                    data_unit = SQLiteDataUnit(row, table_name)
                    principal = self.access_control_policy(data_unit)
                    if principal == None:
                            continue
                    db_bucket_id = self.partition_policy(principal)
                    self.buckets[db_bucket_id] += record_data

    def _parse_table_btree_leaf(self, page: bytes):
        # TODO: make these magic number global constants, similar to the ones for the database header
        # TODO: feed all data that is not part of the record data to metadata stream
        number_of_cells = page[3:5]
        cell_content_area_start = page[5:7]
        cell_pointer_array = page[8:2*number_of_cells]

        for cell_number in (number_of_cells-3):
            cell_start = cell_pointer_array[cell_number*2:cell_number*2+2]

            # First, find size of cell payload, encoded as a varint at the start of the cell (TODO: cite documentation on varint encoding here)
            cell_payload_size_varint = page[cell_start:cell_start+18]
            cell_payload_size, bytes_used_varint_1 = self._varint_to_integer(cell_payload_size_varint)
            
            # Then, find the rowid of this cell, also encoded as a varint
            cell_rowid_varint = page[cell_start+bytes_used_varint_1:cell_start+18]
            cell_rowid, bytes_used_varint_2 = self._varint_to_integer(cell_rowid_varint)

            # We can now process the payload of the cell. The structure is a payload header followed by the record data
            cell_payload = page[cell_start+bytes_used_varint_1+bytes_used_varint_2:cell_start+cell_payload_size]
            payload_header_length_varint = cell_payload[:18]
            payload_header_length, bytes_used_varint_3 = self._varint_to_integer(payload_header_length_varint)
            record_data = cell_payload[payload_header_length:]

            return cell_rowid, record_data
        
    def _find_table_name(self, cur, rowid, record_data):
        # WIP
         # Get the list of tables in the database
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()

        # Prepare a query template with placeholders
        placeholders = ', '.join('?' for _ in record_data)

        # Iterate through each table
        for table in tables:
            table_name = table[0]

            # Generate a query to check if the row exists in the current table
            query = f"SELECT COUNT(*) FROM {table_name} WHERE rowid = ? AND ({', '.join(f'? = ?' for _ in record_data)})"

            try:
                cur.execute(query, (record_data[0], *record_data))
                count = cur.fetchone()[0]

                if count > 0:
                    print(f"Row found in table: {table_name}")
                    cur.close()
                    return table_name
            except sqlite3.Error as e:
                # Handle exceptions if the query fails (like mismatched columns)
                continue

        print("Row not found in any table.")
        cur.close()
        return None


            

    def _varint_to_integer(self, varint):
        result = 0
        shift = 0
        bytes_used = 0

        for b in varint:
            result |= (b & 0x7F) << shift
            bytes_used += 1
            if b & 0x80 == 0:
                break
            shift += 7

        return result, bytes_used
