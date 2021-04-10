#!/usr/bin/env python
import click
import helpers
import random
from os import path as op, system
from s3fs import S3FileSystem
from prefetch.core import S3PrefetchFileSystem

from AFQ import api
import AFQ.data as afd
import AFQ.registration as reg
import AFQ.segmentation as seg
from dipy.io.stateful_tractogram import Origin, Space, StatefulTractogram
import nibabel as nib


def setup():
    MNI_T2_img = afd.read_mni_template()
    img = nib.load("dwi.nii")
    mapping = reg.read_mapping("mapping.nii.gz",img, MNI_T2_img)

    bundles = api.make_bundle_dict(bundle_names=["CST", "ARC"],
                                           resample_to=MNI_T2_img)

    return MNI_T2_img, img, mapping, bundles

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
def segmentation_prefetch(path, lazy, block_size, prefetch_storage, bfile="real.out"):

    MNI_T2_img, img, mapping, bundles = setup()

    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    with fs.open(path, block_size=block_size, prefetch_storage=prefetch_storage, header_bytes=1000) as f:
        tractogram_obj = nib.streamlines.load(f, lazy_load=lazy).tractogram
        print("Done reading")
        streamlines = tractogram_obj.streamlines
        data_per_point = tractogram_obj.data_per_point
        data_per_streamline = tractogram_obj.data_per_streamline
        
        sft = StatefulTractogram(streamlines, img, Space.RASMM,
                                     origin=Origin.NIFTI,
                                     data_per_point=data_per_point,
                                     data_per_streamline=data_per_streamline)

        fiber_groups = segmentation(bundles, sft, mapping, MNI_T2_img)


@helpers.benchmark
def segmentation_s3fs(path, lazy, block_size, bfile="real.out"):

    MNI_T2_img, img, mapping, bundles = setup()

    fs = S3FileSystem()
    fs.invalidate_cache()

    for p in path:
        with fs.open(p, block_size=block_size) as f:
            tractogram_obj = nib.streamlines.load(f, lazy_load=True).tractogram
            streamlines = tractogram_obj.streamlines
            data_per_point = tractogram_obj.data_per_point
            data_per_streamline = tractogram_obj.data_per_streamline
            
            sft = StatefulTractogram(streamlines, img, Space.RASMM,
                                         origin=Origin.NIFTI,
                                         data_per_point=data_per_point,
                                         data_per_streamline=data_per_streamline)

        fiber_groups = segmentation(bundles, sft, mapping, MNI_T2_img)

@click.command()
@click.option(
    "--prefetch_storage",
    nargs=2,
    type=(str, int),
    multiple=True,
    default=[("/dev/shm", 2*1024)],
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

    results_path = "../results/us-west-2-xlarge/"

    bfile = op.join(results_path, f"real_{n_files}f_{reps}r_{block_size}b.out")

    helpers.setup_bench(bfile)

    for r in range(reps):
        random.shuffle(types)
        for t in types:
            print(t)
            helpers.drop_caches()
            
            if t == "s3fs":
                segmentation_s3fs(files, False, block_size, bfile=bfile)
            else:
                segmentation_prefetch(header + files, False, block_size, prefetch_storage, bfile=bfile)

            system("pkill -f joblib")


if __name__=="__main__":
    main()
