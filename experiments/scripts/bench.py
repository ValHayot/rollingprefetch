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


def write_benchmark(output, fs, rep, action, size, time, blocksize=-1, read_size=-1, read_len=None):

    with open(output, "a+") as f:
        f.write(f"{fs},{rep},{action},{size},{time},{blocksize},{read_size},{read_len}\n")


def read_chunks(f, chunk_size, file_size, fs, rep, size, bs, output):

    for i in range(int(file_size // chunk_size)):
        start = perf_counter()
        data = f.read(chunk_size)
        end = perf_counter()

        write_benchmark(output, fs, rep, f"read_{i}", end-start, bs, chunk_size, file_size)

    return end


def bench_aws(size, rep, output, block_size=None, read_size=-1,read_len=None):
    fs = "aws"

    if read_len is None:
        read_len = size

    if read_size == -1:
        read_size = size

    if block_size is None:
        block_size = size

    # clear caches
    helpers.drop_caches()

    s3 = s3fs.S3FileSystem()
    s3.invalidate_cache()

    start_open = perf_counter()

    with s3.open(f"{s3_path}{size}.out", "rb", block_size=block_size) as f:
        end_open = perf_counter()
        end = read_chunks(f, read_size, read_len, fs, rep, size, block_size, output)

    write_benchmark(output, fs, rep, "total", size, end - start_open, block_size, read_size, read_len)
    write_benchmark(output, fs, rep, "open", size, end_open - start_open, block_size, read_size, read_len)


def bench_prefetch(size, rep, output, block_size=None, prefetch_storage=[("/dev/shm", 5*1024**2)], read_size=-1, read_len=None):
    fs = "pf"

    if read_len is None:
        read_len = size

    if read_size == -1:
        read_size = size

    if block_size is None:
        block_size = size

    # clear caches
    helpers.drop_caches()

    s3 = S3PrefetchFileSystem()

    start_open = perf_counter()
    with s3.open(f"{s3_path}{size}.out", "rb", block_size=block_size, prefetch_storage=prefetch_storage) as f:
        end_open = perf_counter()
        end = read_chunks(f, read_size, read_len, fs, rep, size, block_size, output)

    write_benchmark(output, fs, rep, "total", size, end - start_open, block_size, read_size, read_len)
    write_benchmark(output, fs, rep, "open", size, end_open - start_open, block_size, read_size, read_len)


def bench_local(size, rep, fs, output, read_size=-1, read_len=None):

    block_size = -1
    path = f"/dev/shm/rand{size}.out"

    if read_len is None:
        read_len = size

    if read_size == -1:
        read_size = size

    if "local" == fs:
        path = f"/home/ec2-user/rand{size}.out"

    # get file from aws
    sp.run(["aws", "s3", "cp", f"s3://{s3_path}{size}.out", path])

    # clear caches
    helpers.drop_caches()

    # read file and store benchmark in variable
    start_open = perf_counter()
    with open(path, "rb") as f:
        end_open = perf_counter()

        end = read_chunks(f, read_size, read_len, fs, rep, size, block_size, output)

    write_benchmark(output, fs, rep, "total", size, end - start_open, block_size, read_size, read_len)
    write_benchmark(output, fs, rep, "open", size, end_open - start_open, block_size, read_size, read_len)

    # cleanup
    os.unlink(path)


def create_header(output):

    with open(output, "w+") as f:
        f.write("fs,repetition,action,size,time,blocksize,readsize,readlen\n")


def bench_storage():
    filesystems = ["aws", "local", "mem"]
    reps = 20
    output = "../results/us-west-2-xlarge/filetransfer-latency.bench"
    read_size=1024
    read_len=read_size * 20

    create_header(output)
    gen_random(10,32)

    for r in range(reps):
        for i in range(10, 32):
            random.shuffle(filesystems)

            for fs in filesystems:
                size = 2 ** i

                if "aws" in fs:
                    print("executing aws", r, size)
                    bench_aws(size, r, output, read_size=read_size, read_len=read_len)

                else:
                    print("executing", fs, r, size)
                    bench_local(size, r, fs, output, read_size=read_size, read_len=read_len)


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
