#!/usr/bin/env python
import os
import helpers

import s3fs
from prefetch.core import S3PrefetchFileSystem

import random
import subprocess as sp
from time import perf_counter

s3_path = "vhs-bucket/rand"

def gen_random(start, end):

    for i in range(start, end):
        size = 2 ** i
        fname = f"{s3_path}{size}.out"

        # generate a random bytestring
        data = os.urandom(size)

        print("creating randome file", fname, "of size", size)

        fs = s3fs.S3FileSystem()

        with fs.open(fname, "wb") as f:
            f.write(data)


def write_benchmark(output, fs, rep, size, time, blocksize=-1):
    with open(output, "a+") as f:
        f.write(f"{fs},{rep},{size},{time},{blocksize}\n")


def read_chunks(f, chunk_size, file_size):
    for i in range(int((file_size * 2 **20) // chunk_size)):
        data = f.read(chunk_size)


def bench_aws(size, rep, output, block_size=None, read_size=-1):
    fs = "aws"

    if block_size is None:
        block_size = size

    # clear caches
    helpers.drop_caches()

    s3 = s3fs.S3FileSystem()
    s3.invalidate_cache()

    start = perf_counter()

    with s3.open(f"{s3_path}{size}.out", "rb", block_size=block_size) as f:
        if read_size == -1:
            data = f.read()
        else:
            read_chunks(f, read_size, size)

    end = perf_counter()

    write_benchmark(output, fs, rep, size, end - start, block_size)


def bench_prefetch(size, rep, output, block_size=None, prefetch_storage=[("/dev/shm", 5*1024**2)]):
    fs = "prefetch"

    if block_size is None:
        block_size = size

    # clear caches
    helpers.drop_caches()

    s3 = S3PrefetchFileSystem()

    start = perf_counter()
    with s3.open(f"{s3_path}{size}.out", "rb", block_size=block_size, prefetch_storage=prefetch_storage) as f:
        data = f.read()
    end = perf_counter()

    write_benchmark(output, fs, rep, size, end - start, block_size)


def bench_local(size, rep, fs, output):

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

    write_benchmark(output, fs, rep, size, end - start)

    # cleanup
    os.unlink(path)


def create_header(output):

    with open(output, "w+") as f:
        f.write("fs,repetition,size,time,blocksize\n")


def bench_storage():
    filesystems = ["aws", "local", "mem"]
    reps = 20
    output = "../results/us-west-2-xlarge/filetransferpy.bench"

    create_header(output)
    gen_random(10,32)

    for r in range(reps):
        for i in range(10, 32):
            random.shuffle(filesystems)

            for fs in filesystems:
                size = 2 ** i

                if "aws" in fs:
                    print("executing aws", r, size)
                    bench_aws(size, r, output)

                else:
                    print("executing", fs, r, size)
                    bench_local(size, r, fs, output)


def bench_blocksize():
    reps = 1 #5
    bsizes = [2**i for i in range(1, 3)] #12)]
    output = "../results/us-west-2-xlarge/blocksize_s3fs.bench"
    size = 2048

    create_header(output)

    fs = ["s3fs"]#["s3fs"]#, "prefetch"]

    for r in range(reps):
        random.shuffle(bsizes)
        random.shuffle(fs)

        for b in bsizes:

            for f in fs:
                if "s3fs" in f:
                    print("executing s3fs", r, size, b)
                    bench_aws(size, r, output, b*2**20)
                else:
                    print("executing prefetch", r, size, b)
                    bench_prefetch(size, r, output, b*2**20)

#bench_blocksize()
bench_storage()
