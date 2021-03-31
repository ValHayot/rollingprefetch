#!/usr/bin/env python
from nibabel.streamlines import TrkFile
from s3fs import S3FileSystem

import random
import helpers
import os

@helpers.benchmark
def read_mem_nib(paths, lazy, bfile="read_file.bench"):

    for path in paths:
        with open(path, "rb") as f:
            streamlines = TrkFile.load(f, lazy_load=lazy).streamlines


@helpers.benchmark
def read_mem_bytes(paths, lazy, bfile="read_file.bench"):

    for path in paths:
        with open(path, "rb") as f:
            out = f.read()

@helpers.benchmark
def read_s3fs_nib(paths, lazy, block_size, bfile="read_file.bench"):

    fs = S3FileSystem()
    fs.invalidate_cache()

    for path in paths:
        with fs.open(path, "rb", block_size=block_size) as f:
            streamlines = TrkFile.load(f, lazy_load=lazy).streamlines


@helpers.benchmark
def read_s3fs_bytes(paths, lazy, block_size, bfile="read_file.bench"):

    fs = S3FileSystem()
    fs.invalidate_cache()

    for path in paths:
        with fs.open(path, "rb", block_size=block_size) as f:
            out = f.read()


def main():

    lazy=False

    bfile="../results/us-west-2-xlarge/readcmp-1-5f.out"

    reps = 5
    n_files = 5

    types = ["mem", "mem_nib", "s3fs", "s3fs_nib"]
    fs = S3FileSystem()

    all_paths = fs.glob("hydi-tractography/hydi*")
    all_mem_paths = [os.path.join("/dev/shm", os.path.basename(p)) for p in all_paths]

    helpers.setup_bench(bfile)
    for _ in range(reps):

        for i in range(1 ,n_files + 1):
            paths = all_paths[0:i]
            mem_paths = all_mem_paths[0:i]

            random.shuffle(types)

            for t in types:

                helpers.drop_caches()

                if "mem" in t: 
                    fs.get(paths, mem_paths)
                    helpers.drop_caches()

                    if t == "mem":
                        read_mem_bytes(mem_paths, lazy, bfile=bfile)

                    else:
                        read_mem_nib(mem_paths, lazy, bfile=bfile)

                    for p in mem_paths:
                        os.unlink(p)

                else:
                    block_size = 64 * 2 ** 20
                    if "nib" in t:
                        read_s3fs_nib(paths, lazy, block_size, bfile=bfile)
                    else:
                        read_s3fs_bytes(paths, lazy, block_size, bfile=bfile)


if __name__=="__main__":
    main()

