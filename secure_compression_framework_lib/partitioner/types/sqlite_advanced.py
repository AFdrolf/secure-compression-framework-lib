from pathlib import Path
import struct

from secure_compression_framework_lib.partitioner.partitioner import Partitioner


HEADER_SIZE_BYTES = 100
HEADER_STRING = "SQLite format 3\000"




class SQLiteSimplePartitioner(Partitioner):
    """Implements partitioner where the data is a Path object for the SQLite database file to be partitioned."""

    def _get_data(self) -> Path:
        return self.data

    def partition(self) -> list[Path]:
        # INCOMPLETE; adding just basic skeleton. This is the file structure that we need to partition: https://www.sqlite.org/fileformat.html
        with open(self.data, 'rb') as f:
            
            # First, check header
            header = f.read(HEADER_SIZE_BYTES)
            if header[:16].decode('ascii') != HEADER_STRING:
                raise ValueError("Input file is not encoded in SQLite's database file format.")
            page_size = struct.unpack('<H', header[16:18])[0]

        # Next, jump file in [page_size] jumps to iterate over pages
        # Then, parse each page
            # See header of page to get page type
            # Depending on page type, parse page differently (if-statement for each)
                # What we care most about about are btree leaf pages. These require parsing each cell at the bottom of the page, which is what we ultimately feed to different buckets
