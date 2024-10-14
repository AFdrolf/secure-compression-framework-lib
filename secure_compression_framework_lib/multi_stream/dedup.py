"""Implements multi stream deduplication."""
import hashlib
from pathlib import Path
from typing import Callable


def dedup(comparison_function: Callable, file_paths: list[Path]) -> list[Path]:
    """
    Deduplicate the list of input files by comparing them according to some comparison function.

    Args:
        comparison_function: takes as input a string representing the location of a file, and returns some feature of
         the file to be used for comparisons.
         Two files f_1 and f_2 get deduplicated if and only if comparison_function(f_1) == comparison_function(f_2).
         In this case, the first of the files returned by os.walk() is kept.
         A typical example of a comparison function is a hash function such as SHA256.
        file_paths: a list with the file paths of the files to deduplicate

    Returns:
        The file paths of the remaining files after deduplication.

    Todo:
        Generalize to chunk based dedup?
        Verify if comparison_function(file|class_id) is faster?
    """
    features = {}
    for file_path in file_paths:
        features.setdefault(comparison_function(file_path), []).append(file_path)

    deduped_files = []
    for features_files in features.values():
        deduped_files.append(features_files[0])

    return deduped_files


def checksum_comparison_function(file_path: Path, hash_func: Callable = hashlib.sha256, chunk_size: int = 65536):
    """
    Args:
        - hash_func: hash function that supports hashing in chunks via hash.update and hash.hexdigest.

    Todo:
        Could make more efficient by checking size, then each chunk chunk as it is read
    """
    h = hash_func()
    with file_path.open(mode="rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            h.update(data)
    return h.hexdigest()
