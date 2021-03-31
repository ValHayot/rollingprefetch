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
def read_prefetched(path, lazy, block_size, prefetch_storage, bfile="read_file.bench", header_bytes=1000):
    helpers.drop_caches()
    fs = S3PrefetchFileSystem()

    with fs.open(path, block_size=block_size, prefetch_storage=prefetch_storage, header_bytes=header_bytes) as f:
        read_trk(f, lazy, bfile=bfile)


@helpers.benchmark
def read_s3fs(path, lazy, block_size, bfile="read_file.bench"):
    helpers.drop_caches()
    fs = S3FileSystem()

    if isinstance(path, str):
        path = [path]

    for p in path:
        with fs.open(p, block_size=block_size) as f:
            read_trk(f, lazy, bfile=bfile)


def main():

    prefetch_storage = [('/dev/shm', 1*1024)]
    block_size = 64 * 2 ** 20
    n_files = 50

    fs = S3FileSystem()

    header = ["vhs-bucket/hydi-header.trk"]
    files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[:n_files]

    #infile = "hydi-tractography/hydi_tracks.12_58_7.trk" 
    #infile_mem = os.path.join("/dev/shm", os.path.basename(infile))
    lazy=True

    bfile="../results/us-west-2-xlarge/read_50f.out"

    reps = 5

    #types = ["mem", "prefetch", "s3fs"]
    types = ["prefetch", "s3fs"]

    helpers.setup_bench(bfile)
    for _ in range(reps):

        random.shuffle(types)

        for t in types:
            if t == "mem": 
                read_mem(infile_mem, lazy, bfile=bfile)

            elif t == "prefetch":
                print(t)
                read_prefetched(header+files, lazy, block_size, prefetch_storage, bfile=bfile)

            else:
                print(t)
                read_s3fs(files, lazy, block_size, bfile=bfile)


if __name__=="__main__":
    main()

