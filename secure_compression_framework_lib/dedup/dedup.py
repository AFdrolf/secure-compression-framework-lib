"""Library for deduplicating files based on particular partition policies."""
import os

# TODO: generalize to chunk based dedup?
# TODO: verify if comparison_function(file|class_id) is faster?

def dedup(comparison_function, file_paths):
    """
    Deduplicates the list of input files files by comparing them according to some comparison function.

    Args:
        - comparison_function (function): takes as input a string representing the location of a file, and returns some feature of the file to be used for comparisons. Two files f_1 and f_2 get dedup'ed if and only if comparison_function(f_1) == comparison_function(f_2). In this case, the first of the files returned by os.walk() is kept. A typical example of a comparison function is a hash function such as SHA256.
        - file_paths (list): a list with the file paths of the files to deduplicate

    Returns:
        A list with the file paths of the reamining files after deduplication.
    """
    features = {}
    for file_path in file_paths:
        features.setdefault(comparison_function(file_path), []).append(file_path)
    
    deduped_files = []
    for features_files in features.values():
        deduped_files.append(features_files[0])

    return deduped_files


def partition_and_dedup(partition_policy, files_dir, principals, comparison_function):
    """
    Deduplicates the files in each equivalence class as determined by a partition policy.

    Args:
        - partition_policy (function): takes as input the list of file paths and principals and returns a partition of the input file paths. The format of the output is a list of (list, list) pairs, with each pair consisting of (1) a list with the file paths of the files in each class, and (2) a list with the Principal objects of the principals whose files are in this class.
        - files_dir (string): the directory containing the input list of files to deduplicate.
        - principals (list): the input list of principals, represented as Principal objects.
        - comparison_function (function): takes as input files represented as binary streams, and returns some feature of the file to be used for comparisons. Two files f_1 and f_2 get dedup'ed if and only if comparison_function(f_1) == comparison_function(f_2). In this case, the first of the two files to appear in 'files' is kept. A typical example of a comparison function is a hash function such as SHA256.

    Returns:
       A list of (list, list) pairs, with each pair consisting of (1) a list with the file paths of the remaining files after deduplication in each class, and (2) a list with the Principal objects of the principals whose files are in this class.
    """
    file_paths = []
    for root, _, files in os.walk(files_dir):
        for file in files:
            file_path = os.path.join(root, file)
            file_paths.append(file_path)

    classes = partition_policy(file_paths, principals)
    deduped_classes = []
    for c in classes:
        deduped_classes.append((dedup(comparison_function, c[0]), c[1]))
    
    return deduped_classes