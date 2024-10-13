"""Implements multi stream compression."""

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

    def compress(self, data: bytes) -> bytes:
        """Child classes must implement this method."""
        raise NotImplementedError

    def finish(self) -> bytes:
        """Child classes must implement this method."""
        raise NotImplementedError


class DecompressionStream:
    """Base class for decompression."""

    def __init__(self, *parameters) -> None:
        pass

    def decompress(self, data: bytes) -> bytes:
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
        self.compressed = b''
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
        self.decompressed = b''
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
        delimiter: A byte sequence inserted between every call to compress

    Todo:
    ----
        Add support for different compression levels in each stream.
        Add support for multithreading.
        Add support for writing data to file as it is compressed.

    """

    def __init__(self, stream_type: type[CompressionStream], delimiter: bytes = b"||", **stream_params) -> None:
        self.stream_type = stream_type
        self.stream_params = stream_params
        self.compression_streams = {}
        self.stream_switch = []
        self.delimiter = delimiter

    def compress(self, stream_key: str, data: bytes) -> None:
        """Compress data to a given stream.

        Args:
        ----
            stream_key: Label for which compression stream to be used
            data: Data to be compressed

        """
        if not stream_key in self.compression_streams:
            self.compression_streams[stream_key] = self.stream_type(*self.stream_params)

        self.stream_switch.append(stream_key)
        self.compression_streams[stream_key].compress(data + self.delimiter)

    def finish(self) -> tuple[bytes, list[str]]:
        """Flush all compression streams.

        Returns
        -------
            The compressed strings from each stream concatenated together.

        """
        compressed_all = b""
        for compression_stream in self.compression_streams.values():
            compressed_all += compression_stream.finish()
            compressed_all += self.delimiter

        return compressed_all, self.stream_switch


class MSDecompressor:
    """Manages multiple decompression streams.

    Attributes:
    ----------
        stream_type: A DecompressionStream instantiation
        delimiter: The byte sequence used to separate streams

    Todo:
    ----
        Add support for multithreading.

    """

    def __init__(self, stream_type: type[DecompressionStream], delimiter: bytes = b"||") -> None:
        self.stream_type = stream_type
        self.decompression_streams = {}
        self.delimiter = delimiter
        self.stream_switch = None

    def decompress(self, compressed_data: bytes, stream_switch: list[str]) -> None:
        """Decompress until unused_data is found, then start a new DecompressionStream.

        Try to do this: https://stackoverflow.com/questions/58402524/python-zlib-how-to-decompress-many-objects
        """
        self.stream_switch = stream_switch
        for stream_key in stream_switch:
            if stream_key not in self.decompression_streams: self.decompression_streams[
                stream_key] = self.stream_type()

        iterator = iter(range(0, len(compressed_data)))
        stream_key_iter = 0
        to_decompress = b""
        for i in iterator:
            compressed_chunk = compressed_data[i:i + len(self.delimiter)]
            if compressed_chunk != self.delimiter:
                to_decompress += compressed_chunk[0:1]
            else:
                self.decompression_streams[list(self.decompression_streams.keys())[stream_key_iter]].decompress(
                    to_decompress)
                to_decompress = b""
                stream_key_iter += 1
                for _ in range(len(self.delimiter) - 1):
                    next(iterator, None)

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

        decompressed_ordered = b""
        for stream in self.stream_switch:
            decompressed = b""
            i = pointers_stream[stream]
            while True:
                compressed_chunk = self.decompression_streams[stream].decompressed[i:i + len(self.delimiter)]
                if compressed_chunk != self.delimiter:
                    decompressed += compressed_chunk[0:1]
                    i += 1
                else:
                    decompressed_ordered += decompressed
                    pointers_stream[stream] = i + len(self.delimiter)
                    break
        return decompressed_ordered
