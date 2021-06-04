#!/usr/bin/env python
import click
import helpers
import random
from time import perf_counter_ns
from os import path as op, system
from s3fs import S3FileSystem
from prefetch.core import S3PrefetchFileSystem
from dask.distributed import Client, LocalCluster

from AFQ import api
import AFQ.data as afd
import AFQ.registration as reg
import AFQ.segmentation as seg
from dipy.io.streamline import save_tractogram
from dipy.io.stateful_tractogram import Origin, Space, StatefulTractogram
import nibabel as nib
import numpy as np


def seg_setup():
    MNI_T2_img = afd.read_mni_template()
    img = nib.load("dwi.nii")
    mapping = reg.read_mapping("mapping.nii.gz", img, MNI_T2_img)

    bundles = api.make_bundle_dict(
        bundle_names=["CST", "UF", "CC_ForcepsMajor", "CC_ForcepsMinor", "OR", "VOF"],
        seg_algo="reco80",
        resample_to=MNI_T2_img,
    )  # CST ARC

    return {
        "MNI_T2_img": MNI_T2_img,
        "img": img,
        "mapping": mapping,
        "bundles": bundles,
    }


def sgmt(bundles, sft, mapping, MNI_T2_img):
    segmentation = seg.Segmentation(
        return_idx=True,
        seg_algo="reco80",
        reg_algo="syn",
        parallel_segmentation={"engine": "serial"},
        rng=123,
    )  # parallel_segmentation={ "engine": "joblib", "backend": "threading", "n_jobs": 10 }) #
    segmentation.segment(
        bundles,
        sft,
        fdata="dwi.nii",
        fbval="hydi.bval",
        fbvec="hydi.bvec",
        mapping=mapping,
        reg_template=MNI_T2_img,
    )

    return segmentation.fiber_groups


def save_fg(fiber_groups, img, output_dir, bname):
    for kk in fiber_groups:
        print(kk, len(fiber_groups[kk]["sl"].streamlines))
        sft = StatefulTractogram(fiber_groups[kk]["sl"].streamlines, img, Space.RASMM)
        save_tractogram(
            sft, op.join(output_dir, f"{bname}_{kk}_reco.trk"), bbox_valid_check=False
        )


@helpers.benchmark
def segmentation_prefetch(
    path,
    lazy,
    block_size,
    prefetch_storage,
    MNI_T2_img=None,
    ime=None,
    mapping=None,
    bundles=None,
    bfile="real.out",
    output_dir="../outputs",
):
    print("In prefetch", path)
    data = seg_setup()

    # MNI_T2_img, img, mapping, bundles = seg_setup()

    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    start = perf_counter_ns()
    with fs.open(
        path,
        block_size=block_size,
        prefetch_storage=prefetch_storage,
        header_bytes=1000,
    ) as f:
        tfile = nib.streamlines.load(f, lazy_load=lazy)
        # tfile.save(op.join(output_dir, "prefetch.trk"))
        # return "prefetch done"
        tractogram_obj = tfile.tractogram
        streamlines = tractogram_obj.streamlines
        end = perf_counter_ns()

    with open(bfile, "a+") as fo:
        fo.write(
            f'read_prefetch,"{path}",{start},{(end-start)*10**-9},{lazy},{block_size},"{prefetch_storage}"\n'
        )

    data_per_point = tractogram_obj.data_per_point
    data_per_streamline = tractogram_obj.data_per_streamline

    print(
        "data_per_point",
        dict(data_per_point),
        "data_per_streamline",
        dict(data_per_streamline),
    )

    sft = StatefulTractogram(
        streamlines,
        data["img"],
        Space.RASMM,
        origin=Origin.NIFTI,
        data_per_point=data_per_point,
        data_per_streamline=data_per_streamline,
    )

    fiber_groups = sgmt(data["bundles"], sft, data["mapping"], data["MNI_T2_img"])
    save_fg(fiber_groups, data["img"], output_dir, f"prefetch{len(path)-1}")

    return fiber_groups


@helpers.benchmark
def segmentation_s3fs(
    path,
    lazy,
    block_size,
    MNI_T2_img=None,
    ime=None,
    mapping=None,
    bundles=None,
    bfile="real.out",
    output_dir="../outputs",
):
    print("In s3fs", path)
    # MNI_T2_img, img, mapping, bundles
    data = seg_setup()

    fs = S3FileSystem()
    fs.invalidate_cache()

    all_fg = {}

    for i, p in enumerate(path):
        print("iterating over path ", p)
        start = perf_counter_ns()
        with fs.open(p, block_size=block_size) as f:
            tfile = nib.streamlines.load(f, lazy_load=lazy)
            # tfile.save(op.join(output_dir, "s3fs.trk"))
            # return "s3fs done"
            tractogram_obj = tfile.tractogram
            streamlines = tractogram_obj.streamlines
        end = perf_counter_ns()

        with open(bfile, "a+") as fo:
            fo.write(
                f"read_s3fs,{p},{start},{(end-start)*10**-9},{lazy},{block_size},\n"
            )

        data_per_point = tractogram_obj.data_per_point
        data_per_streamline = tractogram_obj.data_per_streamline

        print(
            "data_per_point",
            dict(data_per_point),
            "data_per_streamline",
            dict(data_per_streamline),
        )

        sft = StatefulTractogram(
            streamlines,
            data["img"],
            Space.RASMM,
            origin=Origin.NIFTI,
            data_per_point=data_per_point,
            data_per_streamline=data_per_streamline,
        )

        fiber_groups = sgmt(data["bundles"], sft, data["mapping"], data["MNI_T2_img"])

        for kk in fiber_groups:
            try:
                all_fg[kk]["sl"].streamlines.extend(fiber_groups[kk]["sl"].streamlines)
            except:
                all_fg[kk] = fiber_groups[kk]
    save_fg(all_fg, data["img"], output_dir, f"s3fs{len(path)}")
    return fiber_groups if fiber_groups else {}


@click.command()
@click.option(
    "--prefetch_storage",
    nargs=2,
    type=(str, int),
    multiple=True,
    default=[("/dev/shm", 5 * 1024)],
)
@click.option("--block_size", type=int, default=64 * 2 ** 20)
@click.option("--n_files", type=int, default=5)
@click.option("--reps", type=int, default=5)
@click.option("--types", type=click.Choice(["prefetch", "s3fs"]), multiple=True)
@click.option("--nworkers", type=int, default=3)
def main(prefetch_storage, block_size, n_files, reps, types, nworkers):

    types = list(types)
    header = ["vhs-bucket/hydi-header.trk"]

    fs = S3FileSystem()
    files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[:n_files]
    print(files)

    results_path = "../results/"

    bfile = op.join(
        results_path,
        f"real_{n_files}f_{reps}r_{block_size}b_{nworkers}w-recobundles.out",
    )

    helpers.setup_bench(bfile)

    cluster = LocalCluster(n_workers=nworkers, resources={"CPU": 3})

    client = Client(cluster)

    for r in range(reps):
        # random.shuffle(types)
        for t in types:
            print("***", t, "***")
            helpers.drop_caches()

            print(client)

            data = {}
            results = []

            if t == "s3fs":
                print(t)

                for i in range(nworkers):
                    f_per_w = n_files // nworkers
                    print(files[i * f_per_w : (i + 1) * f_per_w])
                    seg = client.submit(
                        segmentation_s3fs,
                        files[i * f_per_w : (i + 1) * f_per_w],
                        False,
                        block_size,
                        **data,
                        bfile=bfile,
                    )
                    results.append(seg)
            else:
                print(t)

                for i in range(nworkers):
                    f_per_w = n_files // nworkers
                    print(files[i * f_per_w : (i + 1) * f_per_w])
                    seg = client.submit(
                        segmentation_prefetch,
                        header + files[i * f_per_w : (i + 1) * f_per_w],
                        False,
                        block_size,
                        prefetch_storage,
                        **data,
                        bfile=bfile,
                    )
                    results.append(seg)

            print(client.gather(results))
            system("pkill -f joblib")


if __name__ == "__main__":
    main()
