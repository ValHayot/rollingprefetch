#!/usr/bin/env python
from nibabel.streamlines import TrkFile
from prefetch.core import S3PrefetchFileSystem
from s3fs import S3FileSystem

import random
import helpers
import os


@helpers.benchmark
def read_trk(f, lazy, bfile="read_file.bench"):
    streamlines = TrkFile.load(f, lazy_load=lazy).streamlines

    if lazy:
        for stream in streamlines:
            continue

@helpers.benchmark
def read_mem(path, lazy, bfile="read_file.bench"):
    helpers.drop_caches()
    with open(path, "rb") as f:
        read_trk(f, lazy, bfile=bfile)

@helpers.benchmark
def read_prefetched(path, lazy, block_size, prefetch_storage, bfile="read_file.bench"):
    helpers.drop_caches()
    fs = S3PrefetchFileSystem()

    with fs.open(path, block_size=block_size, prefetch_storage=prefetch_storage) as f:
        read_trk(f, lazy, bfile=bfile)


@helpers.benchmark
def read_s3fs(path, lazy, block_size, bfile="read_file.bench"):
    helpers.drop_caches()
    fs = S3FileSystem()

    with fs.open(path, block_size=block_size) as f:
        read_trk(f, lazy, bfile=bfile)


def main():

    prefetch_storage = [('/dev/shm', 5*1024**2)]
    block_size = 64 * 2 ** 20

    infile = "hydi-tractography/hydi_tracks.12_58_7.trk" 
    infile_mem = os.path.join("/dev/shm", os.path.basename(infile))
    lazy=False

    bfile="../results/us-west-2-xlarge/read_12_58_7.out"

    reps = 20

    types = ["mem", "prefetch", "s3fs"]

    helpers.setup_bench(bfile)
    for _ in range(reps):

        random.shuffle(types)

        for t in types:
            if t == "mem": 
                read_mem(infile_mem, lazy, bfile=bfile)

            elif t == "prefetch":
                read_prefetched(infile, lazy, block_size, prefetch_storage, bfile=bfile)

            else:
                read_s3fs(infile, lazy, block_size, bfile=bfile)


if __name__=="__main__":
    main()

