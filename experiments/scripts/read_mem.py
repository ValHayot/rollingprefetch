#!/usr/bin/env python
from nibabel.streamlines import TrkFile

import random
import helpers
import os

@helpers.benchmark
def read_mem(path, lazy, bfile="read_file.bench"):
    helpers.drop_caches()
    with open(path, "rb") as f:
        streamlines = TrkFile.load(f, lazy_load=lazy).streamlines


@helpers.benchmark
def read_nib(path, lazy, bfile="read_file.bench"):
    helpers.drop_caches()

    with open(path, "rb") as f:
        out = f.read()


def main():

    infile = "/dev/shm/hydi_tracks.12_58_7.trk" 
    lazy=False

    bfile="../results/us-west-2-xlarge/readmem_12_58_7.out"

    reps = 5

    types = ["mem", "nib"]

    helpers.setup_bench(bfile)
    for _ in range(reps):

        random.shuffle(types)

        for t in types:
            if t == "mem": 
                read_mem(infile, lazy, bfile=bfile)

            else:
                read_nib(infile, lazy, bfile=bfile)


if __name__=="__main__":
    main()

