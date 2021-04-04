#!/usr/bin/env python
from nibabel.streamlines import TrkFile
from prefetch.core import S3PrefetchFileSystem
from s3fs import S3FileSystem

import random
import helpers
import click
import os


@helpers.benchmark
def read_mem(path, lazy, bfile="read_file.bench"):
    with open(path, "rb") as f:
        helpers.read_trk(f, lazy, bfile=bfile)


@helpers.benchmark
def read_prefetched(
    path, lazy, block_size, prefetch_storage, bfile="read_file.bench", header_bytes=1000
):
    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    with fs.open(
        path,
        block_size=block_size,
        prefetch_storage=prefetch_storage,
        header_bytes=header_bytes,
    ) as f:
        helpers.read_trk(f, lazy, bfile=bfile)


@helpers.benchmark
def read_s3fs(path, lazy, block_size, bfile="read_file.bench"):
    fs = S3FileSystem()
    fs.invalidate_cache()

    if isinstance(path, str):
        path = [path]

    for p in path:
        with fs.open(p, block_size=block_size) as f:
            helpers.read_trk(f, lazy, bfile=bfile)


@click.command()
@click.option(
    "--prefetch_storage",
    nargs=2,
    type=(str, int),
    multiple=True,
    default=[("/dev/shm", 1024)],
)
@click.option("--block_size", type=int, default=64 * 2 ** 20)
@click.option("--n_files", type=int, default=5)
@click.option("--lazy", is_flag=True, default=False)
@click.option("--reps", type=int, default=5)
@click.option("--types", type=click.Choice(["mem", "prefetch", "s3fs"]), multiple=True)
@click.option("--bfile", type=str, default="")
def main(prefetch_storage, block_size, n_files, lazy, reps, types, bfile):

    types = set(types)
    header = ["vhs-bucket/hydi-header.trk"]

    fs = S3FileSystem()
    files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[:n_files]

    results_path = "../results/us-west-2-xlarge/"

    if bfile == "":
        bfile = os.path.join(
            results_path,
            f"readnib_{n_files}f_{'lazy' if lazy else 'nolazy'}_{reps}r_{block_size}b.out",
        )
    else:
        bfile = os.path.join(results_path, bfile)

    helpers.setup_bench(bfile)
    for _ in range(reps):

        random.shuffle(types)

        for t in types:

            print(t)
            helpers.drop_caches()

            if t == "mem":
                mem_files = [
                    os.path.join("/dev/shm", os.path.basename(p)) for p in files
                ]
                fs.get(files, mem_files)
                helpers.drop_caches()
                read_mem(mem_files, lazy, bfile=bfile)

            elif t == "prefetch":
                read_prefetched(
                    header + files, lazy, block_size, prefetch_storage, bfile=bfile
                )

            else:
                read_s3fs(files, lazy, block_size, bfile=bfile)


if __name__ == "__main__":
    main()
