import os
import sqlite3
import struct
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from secure_compression_framework_lib.partitioner.access_control import Principal
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
    "freelist_count_start": 36,
    "freelist_count_end": 39,
}
HEADER_STRING = "SQLite format 3\000"

PAGE_TYPES = {
    "table_leaf": 0x0D,
    "table_interior": 0x05,
    "index_leaf": 0x0A,
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

    def partition(self) -> list[tuple[str, bytes]]:
        bucketed_data = []

        con = sqlite3.connect(self._get_data())
        con.execute("VACUUM")  # Vacuum so we don't need to worry about free pages
        cur = con.cursor()

        # Get schema information
        cur.execute("select name, rootpage FROM sqlite_master WHERE type='table'")
        page_to_table = {row[1]: row[0] for row in cur.fetchall()}
        overflow_pages = []

        db_size = self._get_data().stat().st_size
        with open(self._get_data(), "rb") as f:
            # First, check header, and find page size
            header = f.read(HEADER_SIZE_BYTES)
            if (
                header[
                    HEADER_INFO_POSITIONS["header_string_start"] : HEADER_INFO_POSITIONS["header_string_end"] + 1
                ].decode("ascii")
                != HEADER_STRING
            ):
                raise ValueError("Input file is not encoded in SQLite's database file format.")

            page_size = int.from_bytes(
                header[HEADER_INFO_POSITIONS["page_size_start"] : HEADER_INFO_POSITIONS["page_size_end"] + 1]
            )
            freelist_count = int.from_bytes(
                header[HEADER_INFO_POSITIONS["freelist_count_start"] : HEADER_INFO_POSITIONS["freelist_count_end"] + 1]
            )
            reserved_bytes_per_page = int.from_bytes(header[20:21])
            assert reserved_bytes_per_page == 0

            if freelist_count != 0:
                raise ValueError("Database still contains free pages which could leak data")

            # Main loop: iterate through every page, determine its type, and handle as needed
            for page_number in range(1, db_size // page_size + 1):
                page_start = (page_number - 1) * page_size
                f.seek(page_start)
                page = f.read(page_size)

                # Root page contains the database header and schema information
                if page_number == 1:
                    bucketed_data.append((self.partition_policy(Principal(null=True)), page))
                    continue

                page_type = page[0]
                num_cells = int.from_bytes(page[3:5])
                cell_content_offset = int.from_bytes(page[5:7])

                # Index pages considered metadata
                if page_type == PAGE_TYPES["index_leaf"] or page_type == PAGE_TYPES["index_interior"]:
                    bucketed_data.append((self.partition_policy(Principal(null=True)), page))
                    continue

                # Get name if table page
                table_name = page_to_table[page_number]

                # Parse table interior to add to page_to_table_mapping
                if page_type == PAGE_TYPES["table_interior"]:
                    rightmost_pointer = int.from_bytes(page[8:12])
                    page_to_table[rightmost_pointer] = table_name
                    cell_pointer_array = page[12 : (2 * num_cells)]  # Interior btree page has 12 byte header
                    for cell_index in range(num_cells):
                        cell_offset = int.from_bytes(cell_pointer_array[cell_index * 2 : cell_index * 2 + 2])
                        left_pointer = int.from_bytes(page[cell_offset : cell_offset + 4])
                        page_to_table[left_pointer] = table_name

                    bucketed_data.append((self.partition_policy(Principal(null=True)), page))
                    continue

                # Parse table leaf to partition
                elif page_type == PAGE_TYPES["table_leaf"]:
                    # Page before cell content is metadata
                    bucketed_data.append((self.partition_policy(Principal(null=True)), page[:cell_content_offset]))
                    cell_pointer_array = page[8 : 8 + (2 * num_cells)]
                    cell_offsets = [
                        int.from_bytes(cell_pointer_array[cell_index * 2 : cell_index * 2 + 2])
                        for cell_index in range(num_cells)
                    ]
                    cell_offsets.sort()
                    assert cell_offsets[0] == cell_content_offset
                    for cell_offset in cell_offsets:
                        # First in cell is a varint encoding payload size
                        cell_payload_size, cell_payload_size_bu = _varint_to_integer(
                            page[cell_offset : cell_offset + 9]
                        )
                        payload_on_page = _payload_on_page(page_size, cell_payload_size)

                        # Next is a varint encoding rowid
                        rowid_offset = cell_offset + cell_payload_size_bu
                        cell_rowid, cell_rowid_bu = _varint_to_integer(page[rowid_offset : rowid_offset + 9])

                        cell_payload_offset = cell_payload_size_bu + cell_rowid_bu

                        if payload_on_page < cell_payload_size:
                            # TODO: Handle overflow pages
                            raise ValueError("Overflow pages currently unsupported")
                        else:
                            cell_data = page[cell_offset : cell_offset + cell_payload_offset + payload_on_page]
                            payload = cell_data[cell_payload_offset : cell_payload_offset + payload_on_page]

                        # Now we have the cell decode the payload to find the row data
                        payload_header_size, payload_header_size_bu = _varint_to_integer(payload[:9])
                        payload_header_offset = payload_header_size_bu
                        column_types = []
                        while payload_header_offset < payload_header_size:
                            column_serial_type, column_serial_type_bu = _varint_to_integer(
                                payload[payload_header_offset : payload_header_offset + 9]
                            )
                            column_types.append(column_serial_type)
                            payload_header_offset += column_serial_type_bu

                        record_data = payload[payload_header_size:]
                        record_offset = 0
                        row = []
                        for col in column_types:
                            if col == 0:
                                col_data = None
                            elif col == 8:
                                col_data = 0
                            elif col == 9:
                                col_data = 1
                            else:
                                col_data_size, col_data_type = _get_content_size_type(col)
                                col_data = record_data[record_offset : record_offset + col_data_size]
                                col_data = col_data_type(col_data)
                                record_offset += col_data_size
                            row.append(col_data)

                        data_unit = SQLiteDataUnit(tuple(row), table_name)
                        principal = self.access_control_policy(data_unit)
                        bucketed_data.append((self.partition_policy(principal), cell_data))

                else:
                    raise ValueError("Cannot identify page type")

        return bucketed_data


def _varint_to_integer(varint: bytes) -> tuple[int, int]:
    """Parse a bytestring as a varint and return the varint and bytes used for the encoding"""
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


def _payload_on_page(u: int, p: int):
    """
    Calculate the size of the payload stored on a table btree leaf page (as opposed to on an overflow page).
    The logic is directly copied from sqlite documentation.
    """
    x = u - 35
    m = ((u - 12) * 32 // 255) - 23

    if p <= x:
        return p

    k = m + ((p - m) % (u - 4))

    if k <= x:
        return k
    else:
        return m


def _get_content_size_type(serial_type: int) -> tuple[int, Callable]:
    """Convert a serial type to a content size and a function used to parse content to a Python data type"""
    if serial_type == 0:
        return 0, int.from_bytes  # Value is a NULL.
    elif serial_type == 1:
        return 1, int.from_bytes  # Value is an 8-bit twos-complement integer.
    elif serial_type == 2:
        return 2, int.from_bytes  # Value is a big-endian 16-bit twos-complement integer.
    elif serial_type == 3:
        return 3, int.from_bytes  # Value is a big-endian 24-bit twos-complement integer.
    elif serial_type == 4:
        return 4, int.from_bytes  # Value is a big-endian 32-bit twos-complement integer.
    elif serial_type == 5:
        return 6, int.from_bytes  # Value is a big-endian 48-bit twos-complement integer.
    elif serial_type == 6:
        return 8, int.from_bytes  # Value is a big-endian 64-bit twos-complement integer.
    elif serial_type == 7:
        return 8, int.from_bytes  # Value is a big-endian IEEE 754-2008 64-bit floating point number.
    elif serial_type == 8:
        return 0, int.from_bytes  # Value is the integer 0. (Only available for schema format 4 and higher.)
    elif serial_type == 9:
        return 0, int.from_bytes  # Value is the integer 1. (Only available for schema format 4 and higher.)
    elif serial_type >= 12 and serial_type % 2 == 0:
        return (serial_type - 12) // 2, bytes  # Value is a BLOB that is {(serial_type - 12) // 2} bytes in length.
    elif serial_type >= 13 and serial_type % 2 == 1:
        return (serial_type - 13) // 2, lambda x: x.decode(
            "utf-8"
        )  # Value is a string in the text encoding and {(serial_type - 13) // 2} bytes in length.
