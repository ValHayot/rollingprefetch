#!/user/bin/env python
import math
import click
import helpers
import random
import nibabel as nib
import numpy as np
from time import perf_counter_ns
from os import path as op, system, makedirs
from s3fs import S3FileSystem
from prefetch.core import S3PrefetchFileSystem
from dask.distributed import Client, LocalCluster
from matplotlib import pyplot as plt


def create_fig(output_file, nnodes, nbins):
    bins = np.linspace(
        math.ceil(min(nnodes)), math.floor(max(nnodes)), nbins
    )  # fixed number of bins

    plt.xlim([min(nnodes) - 5, max(nnodes) + 5])

    plt.hist(nnodes, bins=bins, alpha=0.5)
    plt.xlabel("number of nodes per streamline")
    plt.ylabel("count")
    plt.savefig(output_file)


@helpers.benchmark
def histogram_prefetch(
    path,
    lazy,
    block_size,
    prefetch_storage,
    nbins=20,
    output_dir="../outputs",
    bfile="real.out",
):
    print("In prefetch", path)

    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    nnodes = []

    start = perf_counter_ns()
    with fs.open(
        path,
        block_size=block_size,
        prefetch_storage=prefetch_storage,
        header_bytes=1000,
    ) as f:
        tfile = nib.streamlines.load(f, lazy_load=lazy)
        for stream in tfile.streamlines:
            nnodes.append(len(stream))

    output_file = op.join(output_dir, f"histogram_prefetch_{len(path)}.pdf")
    create_fig(output_file, nnodes, nbins)


@helpers.benchmark
def histogram_s3fs(
    path,
    lazy,
    block_size,
    nbins=20,
    output_dir="../outputs",
    bfile="real.out",
):
    print("In s3fs", path)

    fs = S3FileSystem()
    fs.invalidate_cache()

    nnodes = []

    for i, p in enumerate(path):
        with fs.open(p, block_size=block_size) as f:
            tfile = nib.streamlines.load(f, lazy_load=lazy)

            for stream in tfile.streamlines:
                nnodes.append(len(stream))

    output_file = op.join(output_dir, f"histogram_s3fs_{len(path)}.pdf")
    create_fig(output_file, nnodes, nbins)


@click.command()
@click.option("--file_type", type=click.Choice(["orig", "shards"]))
@click.option(
    "--prefetch_storage",
    nargs=2,
    type=(str, int),
    multiple=True,
    default=[("/dev/shm", 2 * 1024)],
)
@click.option("--block_size", type=int, default=64 * 2 ** 20)
@click.option("--n_files", type=int, default=5)
@click.option("--reps", type=int, default=5)
@click.option("--types", type=click.Choice(["prefetch", "s3fs"]), multiple=True)
@click.option("--output_dir", type=str, default="../outputs")
@click.option("--nbins", type=int, default=20)
@click.option("--dask", type=bool, default=False)
@click.option("--nworkers", type=int, default=1)
@click.option("--instance", type=str, default="us-west-2-R5.4xlarge")
@click.option("--lazy", type=bool, default=True)
def main(
    file_type,
    prefetch_storage,
    block_size,
    n_files,
    reps,
    types,
    output_dir,
    nbins,
    dask,
    nworkers,
    instance,
    lazy,
):

    types = list(types)

    fs = S3FileSystem()
    if file_type == "orig":
        header = ["vhs-bucket/hydi-header.trk"]
        files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[:n_files]
    else:
        header = ["vhs-bucket/shards/hydi_shard_header.trk"]
        files = fs.glob("vhs-bucket/shards/hydi_tracks.*.trk")[:n_files]

    results_path = op.join("../results/", instance)

    makedirs(results_path, exist_ok=True)

    bfile = op.join(
        results_path,
        f"histogram_{file_type}_{n_files}f_{reps}r_{block_size}b_{nbins}bins_{nworkers + 'dask' if dask else 'seq'}.out",
    )

    helpers.setup_bench(bfile)

    if dask:
        cluster = LocalCluster(n_workers=nworkers)
        client = Client(cluster)

    for r in range(reps):
        # random.shuffle(types)
        for t in types:
            print(t)
            helpers.drop_caches()

            if dask:

                results = []

                if t == "s3fs":
                    print(t)

                    for i in range(nworkers):
                        f_per_w = n_files // nworkers
                        print(files[i * f_per_w : (i + 1) * f_per_w])
                        seg = client.submit(
                            histogram_s3fs,
                            files[i * f_per_w : (i + 1) * f_per_w],
                            lazy,
                            block_size,
                            nbins=nbins,
                            output_dir=output_dir,
                            bfile=bfile,
                        )
                        results.append(seg)
                else:
                    print(t)

                    for i in range(nworkers):
                        f_per_w = n_files // nworkers
                        print(files[i * f_per_w : (i + 1) * f_per_w])
                        seg = client.submit(
                            histogram_prefetch,
                            header + files[i * f_per_w : (i + 1) * f_per_w],
                            lazy,
                            block_size,
                            prefetch_storage,
                            nbins=nbins,
                            output_dir=output_dir,
                            bfile=bfile,
                        )
                        results.append(seg)

                print(client.gather(results))
            else:
                if t == "s3fs":
                    histogram_s3fs(
                        files,
                        lazy,
                        block_size,
                        nbins=nbins,
                        output_dir=output_dir,
                        bfile=bfile,
                    )
                else:
                    histogram_prefetch(
                        header + files,
                        lazy,
                        block_size,
                        prefetch_storage,
                        nbins=nbins,
                        output_dir=output_dir,
                        bfile=bfile,
                    )


if __name__ == "__main__":
    main()