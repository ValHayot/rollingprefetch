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
from dipy.io.stateful_tractogram import Origin, Space, StatefulTractogram
import nibabel as nib


def seg_setup():
    MNI_T2_img = afd.read_mni_template()
    img = nib.load("dwi.nii")
    mapping = reg.read_mapping("mapping.nii.gz",img, MNI_T2_img)

    bundles = api.make_bundle_dict(bundle_names=["CST", "ARC"],
                                           resample_to=MNI_T2_img)

    return { "MNI_T2_img": MNI_T2_img, "img": img, "mapping": mapping, "bundles": bundles }

def segmentation(bundles, sft, mapping, MNI_T2_img):
    segmentation = seg.Segmentation(return_idx=True) #, parallel_segmentation={"engine": "serial"})
    segmentation.segment(bundles,
                         sft,
                         fdata="dwi.nii",
                         fbval="hydi.bval",
                         fbvec="hydi.bvec",
                         mapping=mapping,
                         reg_template=MNI_T2_img)

    return segmentation.fiber_groups


@helpers.benchmark
def segmentation_prefetch(path, lazy, block_size, prefetch_storage, MNI_T2_img=None, ime=None, mapping=None, bundles=None, bfile="real.out"):
    print("In prefetch", path)
    data = seg_setup()

    #MNI_T2_img, img, mapping, bundles = seg_setup()

    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    with fs.open(path, block_size=block_size, prefetch_storage=prefetch_storage, header_bytes=1000) as f:
        start = perf_counter_ns()
        tractogram_obj = nib.streamlines.load(f, lazy_load=lazy).tractogram
        streamlines = tractogram_obj.streamlines
        end = perf_counter_ns()

        with open(f"read_benchmarks_{op.basename(bfile)}", "a+") as fo:
            fo.write(f"prefetch,{path},{end-start}\n")

        data_per_point = tractogram_obj.data_per_point
        data_per_streamline = tractogram_obj.data_per_streamline
        
        sft = StatefulTractogram(streamlines, data["img"], Space.RASMM,
                                     origin=Origin.NIFTI,
                                     data_per_point=data_per_point,
                                     data_per_streamline=data_per_streamline)

        fiber_groups = segmentation(data["bundles"], sft, data["mapping"], data["MNI_T2_img"])

        return fiber_groups


@helpers.benchmark
def segmentation_s3fs(path, lazy, block_size, MNI_T2_img=None, ime=None, mapping=None, bundles=None, bfile="real.out"):
    print("In s3fs", path)
    #MNI_T2_img, img, mapping, bundles
    data = seg_setup()

    fs = S3FileSystem()
    fs.invalidate_cache()

    for p in path:
        print("iterating over path ", p)
        with fs.open(p, block_size=block_size) as f:
            start = perf_counter_ns()
            tractogram_obj = nib.streamlines.load(f, lazy_load=False).tractogram
            streamlines = tractogram_obj.streamlines
            end = perf_counter_ns()

            with open(f"read_benchmarks_{op.basename(bfile)}", "a+") as fo:
                fo.write(f"s3fs,{path},{end-start}\n")

            data_per_point = tractogram_obj.data_per_point
            data_per_streamline = tractogram_obj.data_per_streamline
            
            sft = StatefulTractogram(streamlines, data["img"], Space.RASMM,
                                         origin=Origin.NIFTI,
                                         data_per_point=data_per_point,
                                         data_per_streamline=data_per_streamline)

        fiber_groups = segmentation(data["bundles"], sft, data["mapping"], data["MNI_T2_img"])
        return fiber_groups


@click.command()
@click.option(
    "--prefetch_storage",
    nargs=2,
    type=(str, int),
    multiple=True,
    default=[("/dev/shm", 5*1024)],
)
@click.option("--block_size", type=int, default=64 * 2 ** 20)
@click.option("--n_files", type=int, default=5)
@click.option("--reps", type=int, default=5)
@click.option("--types", type=click.Choice(["prefetch", "s3fs"]), multiple=True)
def main(prefetch_storage, block_size, n_files, reps, types):

    types = list(types)
    header = ["vhs-bucket/hydi-header.trk"]

    fs = S3FileSystem()
    files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[:n_files]
    print(files)

    results_path = "../results/"

    bfile = op.join(results_path, f"real_{n_files}f_{reps}r_{block_size}b.out")

    helpers.setup_bench(bfile)

    cluster = LocalCluster(n_workers=3, threads_per_worker=1)

    client = Client(cluster)

    for r in range(reps):
        random.shuffle(types)
        for t in types:
            print("***",t,"***")
            helpers.drop_caches()

            print(client)

            data = {}
            results = []

            if t == "s3fs":
                print(t)

                for i in range(n_files):
                    print(files[i:i+1])
                    seg = client.submit(segmentation_s3fs, files[i:i+1], False, block_size, **data, bfile=bfile)
                    results.append(seg)
            else:
                print(t)

                for i in range(n_files):
                    print(files[i:i+1])
                    seg = client.submit(segmentation_prefetch, header + files[i:i+1], False, block_size, prefetch_storage, **data, bfile=bfile)
                    results.append(seg)

            print(client.gather(results))
            system("pkill -f joblib")


if __name__=="__main__":
    main()
