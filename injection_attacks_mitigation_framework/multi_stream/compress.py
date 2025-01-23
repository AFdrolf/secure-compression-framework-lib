"""Implements multi stream compression."""

import json
import zlib

from typing_extensions import override


class StreamClosedException(Exception):
    """Custom exception for use when a compression stream has been closed."""

    def __init__(self):
        super().__init__("Stream closed")


class CompressionStream:
    """Base class for compression."""

    def __init__(self, *parameters) -> None:
        pass

    def compress(self, data: bytes) -> None:
        """Child classes must implement this method."""
        raise NotImplementedError

    def finish(self) -> bytes:
        """Child classes must implement this method."""
        raise NotImplementedError


class DecompressionStream:
    """Base class for decompression."""

    def __init__(self, *parameters) -> None:
        pass

    def decompress(self, data: bytes) -> None:
        """Child classes must implement this method."""
        raise NotImplementedError

    def finish(self) -> bytes:
        """Child classes must implement this method."""
        raise NotImplementedError


class ZlibCompressionStream(CompressionStream):
    """Wraps zlib compression.

    Attributes
    ----------
        compression_object: A zlib compressobj.
        compressed: The compression stream.
        finished: Boolean indicating whether stream is finished.

    """

    def __init__(self, level: int = -1) -> None:
        super().__init__()
        self.compression_object = zlib.compressobj(level=level)
        self.compressed = b""
        self.finished = False

    @override
    def compress(self, data: bytes) -> None:
        if self.finished:
            raise StreamClosedException
        c = self.compression_object.compress(data)
        self.compressed += c

    @override
    def finish(self) -> bytes:
        """Return the entire compression of input bytes."""
        if self.finished:
            raise StreamClosedException
        self.compressed += self.compression_object.flush()
        self.finished = True
        return self.compressed


class ZlibDecompressionStream(DecompressionStream):
    """Wraps zlib decompression.

    Attributes
    ----------
        decompression_object: A zlib decompressobj.
        decompressed: The decompression stream.
        finished: Boolean indicating whether stream is finished.

    """

    def __init__(self):
        super().__init__()
        self.decompression_object = zlib.decompressobj()
        self.decompressed = b""
        self.finished = False

    @override
    def decompress(self, compressed_data: bytes) -> None:
        if self.finished:
            raise StreamClosedException
        d = self.decompression_object.decompress(compressed_data)
        self.decompressed += d

    @override
    def finish(self) -> bytes:
        if self.finished:
            raise StreamClosedException
        self.decompressed += self.decompression_object.flush()
        self.finished = True
        return self.decompressed


class MSCompressor:
    """Manages multiple compression streams.

    Attributes:
    ----------
        stream_type: A CompressionStream instantiation
        stream_params: Parameters for CompressionStream
        stream_switch_delimiter: A byte sequence inserted between every call to compress as an indication to switch
        streams while decompressing (currently this must be a byte sequence not found in the data to be compressed).
        This sequence should not include duplicate bytes - imagine we use '||', this would cause an error because if
        a '|' in the data ends up next to the delimiter we cannot determine where the true delimiter is
        output_delimiter: A byte sequence used to separate each compression stream in the output


    """

    def __init__(
        self,
        stream_type: type[CompressionStream],
        stream_switch_delimiter: bytes = b"[|",
        output_delimiter: bytes = b"\x7f",
        **stream_params,
    ) -> None:
        self.stream_type = stream_type
        self.stream_params = stream_params
        self.compression_streams = {}
        self.stream_switch = []
        if len(stream_switch_delimiter) != len(set(stream_switch_delimiter)):
            raise ValueError("Delimiter should be unique characters")
        self.stream_switch_delimiter = stream_switch_delimiter
        self.output_delimiter = output_delimiter

    def compress(self, stream_key: str, data: bytes) -> None:
        """Compress data to a given stream.

        Args:
        ----
            stream_key: Label for which compression stream to be used
            data: Data to be compressed

        """
        if self.stream_switch_delimiter in data:
            raise ValueError("Delimiter found in data")

        if not stream_key in self.compression_streams:
            self.compression_streams[stream_key] = self.stream_type(*self.stream_params)

        self.stream_switch.append(stream_key)
        self.compression_streams[stream_key].compress(data + self.stream_switch_delimiter)

    def encode_remove_output_delimiter(self, data: bytes) -> bytes:
        """Removes output delimiter from compressed data.

        If the output delimiter shows up in compressed data we will interpret this as changing
        streams and the header check will fail. Here we replace a chosen delimiter byte by a two byte escape sequence
        and replace the prefix of the escape sequence by a different two byte sequence as suggested in
        https://stackoverflow.com/questions/62585234/can-zlib-compressed-output-avoid-using-certain-byte-value
        """
        return data.replace(b"Z", b"ZZ").replace(self.output_delimiter, b"Z:")

    def finish(self) -> bytes:
        """Flush all compression streams.

        Returns
        -------
            The bytes for a json encoding of the stream_switch list followed by the compressed strings from each stream
            concatenated together.

        """
        compressed_all = json.dumps(self.stream_switch).encode("utf-8") + self.output_delimiter
        for k, compression_stream in self.compression_streams.items():
            compressed_all += self.encode_remove_output_delimiter(compression_stream.finish())
            compressed_all += self.output_delimiter

        return compressed_all


class MSDecompressor:
    """Manages multiple decompression streams.

    Attributes:
    ----------
        stream_type: A DecompressionStream instantiation
        stream_switch_delimiter: A byte sequence inserted between every call to compress as an indication to switch
        streams while decompressing (currently this must be a byte sequence not found in the data to be compressed).
        This sequence should not include duplicate bytes - imagine we use '||', this would cause an error because if
        a '|' in the data ends up next to the delimiter we cannot determine where the true delimiter is
        output_delimiter: A byte sequence used to separate each compression stream in the output


    """

    def __init__(
        self, stream_type: type[DecompressionStream], stream_switch_delimiter: bytes = b"[|", output_delimiter=b"\x7f"
    ) -> None:
        self.stream_type = stream_type
        self.decompression_streams = {}
        if len(stream_switch_delimiter) != len(set(stream_switch_delimiter)):
            raise ValueError("Delimiter should be unique characters")
        self.stream_switch_delimiter = stream_switch_delimiter
        self.stream_switch = None
        self.output_delimiter = output_delimiter

    def decode_add_output_delimiter(self, data: bytes) -> bytes:
        """Adds output delimiter occurrences to compressed data.

        In MSCompressor we escaped the output delimiter so that it did not occur in the compressed data and could be
        used as a delimiter. This function parses data to find the escape sequences and replaces occurrences.
        """
        output = []
        pair = False
        for b in data:
            if pair:
                # Previous byte was escape prefix
                # 90 -> b'Z'
                o = 90 if b == 90 else int.from_bytes(self.output_delimiter)
                output.append(o)
                pair = False
            elif b == 90:
                pair = True
            else:
                output.append(b)
        return bytes(output)

    def decompress(self, compressed_data: bytes) -> None:
        """Decompress until unused_data is found, then start a new DecompressionStream.

        Try to do this: https://stackoverflow.com/questions/58402524/python-zlib-how-to-decompress-many-objects
        """
        iterator = iter(range(0, len(compressed_data)))
        stream_key_iter = 0
        prev_od_pos = -1
        od_pos = -1
        to_decompress = b""
        decoded_stream_switch = False
        while -1 != (od_pos := compressed_data.find(self.output_delimiter, od_pos + 1)):
            to_decompress += compressed_data[prev_od_pos + len(self.output_delimiter) : od_pos]
            if not decoded_stream_switch:
                # Data before first output delimiter should be stream switch
                try:
                    stream_switch = json.loads(to_decompress.decode("utf-8"))
                except json.decoder.JSONDecodeError:
                    raise ValueError("Expected JSON encoding of stream_switch list")
                self.stream_switch = stream_switch
                for stream_key in stream_switch:
                    if stream_key not in self.decompression_streams:
                        self.decompression_streams[stream_key] = self.stream_type()
                decoded_stream_switch = True
            else:
                self.decompression_streams[list(self.decompression_streams.keys())[stream_key_iter]].decompress(
                    self.decode_add_output_delimiter(to_decompress)
                )
                stream_key_iter += 1
            to_decompress = b""
            prev_od_pos = od_pos

    def finish(self) -> bytes:
        """Flush all decompression streams.

        Returns
        -------
            The decompressed strings from each stream concatenated together.

        """
        pointers_stream = {}
        for stream_key, decompression_stream in self.decompression_streams.items():
            pointers_stream[stream_key] = 0
            decompression_stream.finish()

        split_decompressed_bytes = {
            stream_key: self.decompression_streams[stream_key].decompressed.split(self.stream_switch_delimiter)
            for stream_key in self.decompression_streams.keys()
        }
        decompressed_ordered = bytearray()
        for stream in self.stream_switch:
            decompressed_ordered.extend(split_decompressed_bytes[stream].pop(0))

        return decompressed_ordered
