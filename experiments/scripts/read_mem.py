#!/usr/bin/env python
from nibabel.streamlines import TrkFile
from s3fs import S3FileSystem

import random
import helpers
import os

@helpers.benchmark
def read_nib(paths, lazy, bfile="read_file.bench"):
    helpers.drop_caches()

    for path in paths:
        with open(path, "rb") as f:
            streamlines = TrkFile.load(f, lazy_load=lazy).streamlines


@helpers.benchmark
def read_mem(paths, lazy, bfile="read_file.bench"):
    helpers.drop_caches()

    for path in paths:
        with open(path, "rb") as f:
            out = f.read()


def main():

    lazy=False

    bfile="../results/us-west-2-xlarge/readmem.out"

    reps = 5
    n_files = 5

    types = ["mem", "nib"]
    fs = S3FileSystem()

    all_paths = fs.glob("hydi-tractography/hydi*")
    all_mem_paths = [os.path.join("/dev/shm", os.path.basename(p)) for p in all_paths]

    helpers.setup_bench(bfile)
    for _ in range(reps):

        for i in range(1 ,n_files + 1):
            paths = all_paths[0:i]
            mem_paths = all_mem_paths[0:i]

            fs.get(paths, mem_paths)

            random.shuffle(types)

            for t in types:
                if t == "mem": 
                    read_mem(mem_paths, lazy, bfile=bfile)

                else:
                    read_nib(mem_paths, lazy, bfile=bfile)

            for p in mem_paths:
                os.unlink(p)


if __name__=="__main__":
    main()

