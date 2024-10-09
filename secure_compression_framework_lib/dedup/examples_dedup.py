import hashlib
import os
import random
import shutil
import sys

import dedup
sys.path.append(sys.path[0] + '/..')
from lib import Principal


############################ 
# Setup
############################
def generate_files(number_of_files, max_file_size, output_dir, classes_of_duplicates=None, filename_prefix="test_file_"):
    """
    Args:
        - classes_of_duplicates (list): if specified, defines which input files should be duplicates. Format is a list of integers, which each integer denoting the size of one set of duplicate files. The integers must sum to less than or equal to number_of_files. If less than number_of_files, all remaining files will be unique. For example, [5, 7, 2] indicates that 5 files are the same, 7 other files are the same, and 2 other files are the same.
        If input is not specified, pick a random number n between [1, number_of_files], and then pick a random partition of the list [1, ..., n].
    """
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    if classes_of_duplicates and not number_of_files >= sum(classes_of_duplicates):
        print("Error! Number of total files must be greater than number of duplicate files. Input number of total files is %s, and duplicate files add up to %s" % (number_of_files, classes_of_duplicates))
        return None

    if not classes_of_duplicates:
        number_of_duplicates = random.randrange(1, number_of_files+1)
        classes_of_duplicates = []
        while number_of_duplicates - sum(classes_of_duplicates) > 1:
            classes_of_duplicates.append(random.randrange(2, number_of_duplicates - sum(classes_of_duplicates)+1))

    ix = 0
    for duplicate_class in classes_of_duplicates:
        file_path_ref = os.path.join(output_dir, filename_prefix + str(ix))
        size = random.randrange(1, max_file_size+1)
        with open(file_path_ref, 'wb') as f:
                f.write(os.urandom(size))
        ix += 1
        for _ in range(1, duplicate_class):
            file_path = os.path.join(output_dir, filename_prefix + str(ix))
            shutil.copyfile(file_path_ref, file_path)
            ix += 1
    
    for _ in range(number_of_files - sum(classes_of_duplicates)):
        file_path = os.path.join(output_dir, filename_prefix + str(ix))
        size = random.randrange(1, max_file_size+1)
        with open(file_path, 'wb') as f:
                f.write(os.urandom(size))
        ix += 1

    return classes_of_duplicates

def generate_principals_contacts_and_sent_files(number_of_principals, files_dir):
    """
    Generates 'number_of_principal' Principal objects with three attributes: (1) 'id', which is an integer that serves as a unique identifier for this principal; (2) 'sent_files', which is a list of file paths for the files sent by this principal; and (3) 'contact' which is a boolean indicating if this principal is a contact or not. Files from 'files_dir' are assigned randomly to each principal, and each principal is randomly assigned to be a contact or not.

    Args:
        - files_dir (string): the directory containing the input list of files to assign to the principals.
    """
    principals = []
    for i in range(number_of_principals):
        principal = Principal()
        setattr(principal, 'id', i)
        setattr(principal, 'contact', bool(random.getrandbits(1)))
        setattr(principal, 'sent_files', [])
        principals.append(principal)
    
    for root, _, files in os.walk(files_dir):
        for file in files:
            file_path = os.path.join(root, file)
            getattr(principals[random.randrange(number_of_principals)], 'sent_files').append(file_path)

    return principals


############################ 
# Examples of partition policies
############################
def sender_based_partition_policy(files, principals):
    """
    Args:
        - principals: List of Principal objects. Requires that each principal object have an attribute labelled 'sent_files' which is a list of file paths for the files sent by this principal. Each file in 'files' must appear in one and only one 'sent_files' list.
    
    Returns:
        A list of (list, list) pairs, with each pair consisting of (1) a list with the file paths of the files in each class, and (2) a list with the Principal objects of the principals whose files are in this class.
    """
    number_of_files_principals = 0
    classes = []
    for i, principal in enumerate(principals):
        try:
            files_principal = getattr(principal, "sent_files")
            classes.append((files_principal, [principal]))
            number_of_files_principals += len(files_principal)
            for file in files_principal:
                if file not in files:
                    print("Error! File %s from Principal %s not in input list of files" % (file, i))
                    return None
        except AttributeError:
            print("Error! Principal object %s does not have a 'files' attribute" % i)
            return None
    
    number_of_files = len(files)
    if number_of_files_principals != number_of_files:
        print("Error! Number of files is %s, but number of files from principals is %s" % (number_of_files, number_of_files_principals))
    
    return classes

def contacts_based_partition_policy(files, principals):
    number_of_files_principals = 0
    # classes[0] are the files for the contacts, and each non-contact gets its own individual class.
    classes = [([],[])]
    for i, principal in enumerate(principals):
        try:
            files_principal = getattr(principal, "sent_files")
            if principal.contact == True:
                classes[0][0].extend(files_principal)
                classes[0][1].extend([principal])
            else:
               classes.append((files_principal, [principal]))
            number_of_files_principals += len(files_principal)
            for file in files_principal:
                if file not in files:
                    print("Error! File %s from Principal %s not in input list of files" % (file, i))
                    return None
        except AttributeError:
            print("Error! Principal object %s does not have a 'files' attribute" % i)
            return None
    
    number_of_files = len(files)
    if number_of_files_principals != number_of_files:
        print("Error! Number of files is %s, but number of files from principals is %s" % (number_of_files, number_of_files_principals))
    
    return classes


############################ 
# Examples of comparison functions
############################
def checksum_comparison_function(file_path, hash=hashlib.sha256, chunk_size=65536):
    """
    Args:
        - hash (function): hash function that supports hashing in chunks via hash.update and hash.hexdigest.
    """
    h = hash()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

def efficient_checksum_comparison_function(file_path, hash=hashlib.sha256, chunk_size=65536):
    # TODO. Check if size of file is the same, then check first chunk_size bytes, then check entire hash.
    pass


def run_example(partition_policy, files_dir, principals, comparison_function):
    return dedup.partition_and_dedup(partition_policy, files_dir, principals, comparison_function)


if __name__ == "__main__":
    # Set parameters. TODO: parse as command line arguments and add a main() function.
    number_of_files = 7
    max_file_size = 100
    classes_of_duplicates = [7]
    output_dir = "/Users/andresfg/Desktop/Cornell/Research/secure-processing-framework-project/secure-compression-framework-lib/dedup-lib/test-files"
    number_of_principals = 4
    filename_prefix="test_file_"

    hash = hashlib.sha256
    chunk_size=65536
    partition_policy = contacts_based_partition_policy
    comparison_function = lambda file_path: checksum_comparison_function(file_path, hash, chunk_size)


    # Generate files
    classes_of_duplicates = generate_files(number_of_files, max_file_size, output_dir, classes_of_duplicates, filename_prefix)
    print("- DUPLICATE FILES:")
    if classes_of_duplicates == []:
        print("\tNo duplicate files")
    prev_class = 0 
    for duplicate_class in classes_of_duplicates:
        print("\tFiles %s are duplicates" % [filename_prefix + str(prev_class + i)for i in range(duplicate_class)])
        prev_class = duplicate_class


    # Generate principals
    principals = generate_principals_contacts_and_sent_files(number_of_principals, output_dir)
    print("\n- PRINCIPALS:")
    for principal in principals:
        print("\tPrincipal %s sent files %s and %s a contact" % (principal.id, [f[f.rfind("/")+1:] for f in principal.sent_files], "IS" if principal.contact else "IS NOT"))


    # Run example
    deduped_files = run_example(partition_policy, output_dir, principals, comparison_function)
    print("\n- DEDUPLICATED FILES (%s):" % partition_policy.__name__)
    for i, dedup_class in enumerate(deduped_files):
        print("\tClass %s (principals %s): %s" % (i, ", ".join([str(f.id) for f in dedup_class[1]]),[f[f.rfind("/")+1:] for f in dedup_class[0]]))

    



    

    