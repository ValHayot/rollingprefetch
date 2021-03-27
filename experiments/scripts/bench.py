#!/usr/bin/env python
import os
import helpers
import s3fs
import random
import subprocess as sp
from time import perf_counter

output = "../results/us-west-2-xlarge/filetransferpy.bench"
s3_path = "vhs-bucket/rand"


def write_benchmark(fs, rep, size, time):
    with open(output, "a+") as f:
        f.write(f"{fs},{rep},{size},{time}\n")


def bench_aws(size, rep):
    fs = "aws"

    # clear caches
    helpers.drop_caches()

    s3 = s3fs.S3FileSystem()

    start = perf_counter()
    with s3.open(f"{s3_path}{size}.out", "rb", block_size= size * 2 **20) as f:
        data = f.read()
    end = perf_counter()

    write_benchmark(fs, rep, size, end - start)


def bench_local(size, rep, fs):

    path = f"/dev/shm/rand{size}.out"

    if "local" == fs:
        path = f"/home/ec2-user/rand{size}.out"

    # get file from aws
    sp.run(["aws", "s3", "cp", f"s3://{s3_path}{size}.out", path])

    # clear caches
    helpers.drop_caches()

    # read file and store benchmark in variable
    start = perf_counter()
    with open(path, "rb") as f:
        data = f.read()
    end = perf_counter()

    write_benchmark(fs, rep, size, end - start)

    # cleanup
    os.unlink(path)


filesystems = ["aws", "local", "mem"]
reps = 5

with open(output, "w+") as f:
    f.write("fs,repetition,size,time")

for r in range(reps):
    for i in range(1, 12):
        random.shuffle(filesystems)

        for fs in filesystems:
            size = 2 ** i

            if "aws" in fs:
                print("executing aws", r, size)
                bench_aws(size, r)

            else:
                print("executing", fs, r, size)
                bench_local(size, r, fs)
