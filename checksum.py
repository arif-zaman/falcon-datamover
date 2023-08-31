'''
1. list files from the directory
2. parallelly calculate md5 hash of each file
3. sort the hash files by filenames
4. calculate md5 hash of the hashes
Run: python3 checksum.py --dir /directory/path/
'''


import os
import time
import glob
import pathlib
import hashlib
import argparse
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor


# Get the list of files of a directory
def parse_files(root_dir):
    print(f"Parsing files from {root_dir} ....")
    files = []

    if pathlib.Path(root_dir).exists():
        root_dir = root_dir if root_dir[-1] == "/" else root_dir + "/"

        for fpath in glob.glob(root_dir + "**", recursive=True):
            if os.path.isfile(fpath):
                files.append(fpath)

    # file_sizes = [os.path.getsize(file) for file in files]
    # print(sorted(zip(file_sizes,files))[::-1])
    return files


# Calculate MD5 hash of a file
def get_hash(file_path, file_no):
    start = time.time()
    md5 = hashlib.md5()
    chunk_size = 1024*1024

    with open(file_path, 'rb') as ff:
        data = ff.read(chunk_size)
        while data:
            md5.update(data)
            data = ff.read(chunk_size)

    hash_value = md5.hexdigest()
    end = time.time()

    print(f"{file_no} >> {file_path}, hash={hash_value}, time={round(end-start, 1)} sec")
    return (file_path, hash_value)


# Get final checksum value of all files
def get_checksum(files):
    print(f"Running checksum calculation of {len(files)} files ....")
    start = time.time()

    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        tasks = []
        for i in range(len(files)):
            tasks.append(executor.submit(get_hash, files[i], i+1))

        hash_values = {}
        for task in tasks:
            filaname, md5_hash = task.result()
            hash_values[filaname] = md5_hash

    # Calculate checksum from the hashes of sorted filenames
    md5 = hashlib.md5()
    for file in sorted(hash_values.keys()):
        md5.update(hash_values[file].encode())

    print()
    print(f"Combined Checksum of {len(files)} files = {md5.hexdigest()}")
    print(f"Total checksum calculation time: {round(time.time() - start, 1)} sec")


if __name__ == "__main__":
    root_dir = None
    parser=argparse.ArgumentParser()
    parser.add_argument("--dir", help="directory path")
    args = vars(parser.parse_args())

    if args["dir"]:
        root_dir = args["dir"]

    if root_dir:
        files = parse_files(root_dir)

        if files:
            get_checksum(files)
        else:
            print("The directory path is invalid or empty!")
    else:
        print("please provide directory path")
        print("Run: python3 checksum.py --dir /directory/path/")