from collections import defaultdict
import os
from pathlib import Path
import struct

from secure_compression_framework_lib.partitioner.partitioner import Partitioner

HEADER_SIZE_BYTES = 100
PAGE_SIZE_INFO_START_BYTE = 16
PAGE_SIZE_INFO_END_BYTE = 18
# Each start,end is inclusive
HEADER_INFO_POSITIONS = {
    "header_string_start":0,
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

class SQLiteAdvancedPartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for the SQLite database file to be partitioned."""

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> list[Path]:
        buckets = defaultdict(bytes)
        bucket_paths = []

        db_size = os.path.getsize(self.data)
        with open(self.data, "rb") as f:
            # First, check header, and find page size
            header = f.read(HEADER_SIZE_BYTES)
            if header[HEADER_INFO_POSITIONS["header_string_start"]:HEADER_INFO_POSITIONS["header_string_end"]+1].decode("ascii") != HEADER_STRING:
                raise ValueError("Input file is not encoded in SQLite's database file format.")
            page_size = struct.unpack("<H", header[HEADER_INFO_POSITIONS["page_size_start"]:HEADER_INFO_POSITIONS["page_size_end"]+1])[0]
            free_list_first_page = header[HEADER_INFO_POSITIONS["freelist_first_page_number_start"]:HEADER_INFO_POSITIONS["freelist_first_page_number_start"]+1]

            # We need to keep track of freelist and overflow pages, since there is no identifier for these
            # TODO: maybe we do not actually care about freelist pages? 
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
                    buckets["metadata"] += page
                # Locking page, which always contains all zeroes
                elif page_number == 2**30:
                    buckets["metadata"] += page
                # Either table interior, index interior, or index b-tree leaf page. These do not contain cell data
                elif page_type in PAGE_TYPES.values() and page_type != PAGE_TYPES["table_leaf"]:
                    buckets["metadata"] += page
                # Table b-tree leaf page
                elif page_type == PAGE_TYPES["table_leaf"]:
                    self._parse_btree_leaf(page)

    def _parse_table_btree_leaf(self, page: bytes):
        # TODO: make these magic number global constants, similar to the ones for the database header
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
            cell_rowid, bytes_used_varint_2 = self._varint_to_integer(cell_payload_size_varint)

            # We can now process the payload of the cell. The structure is a payload header followed by the record data
            cell_payload = page[cell_start+bytes_used_varint_1+bytes_used_varint_2:cell_start+cell_payload_size]
            payload_header_length_varint = cell_payload[:18]
            payload_header_length, bytes_used_varint_3 = self._varint_to_integer(payload_header_length_varint)
            record_data = cell_payload[payload_header_length:]

            # Finally, the record data contains the contents of the database cells
            


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
