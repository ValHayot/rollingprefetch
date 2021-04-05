import sys

from nibabel.streamlines import TrkFile
from s3fs import S3FileSystem

import helpers


def read_trk(f, lazy, bfile="read_file.bench"):
    streamlines = TrkFile.load(f, lazy_load=lazy).streamlines

    if lazy:
        for stream in streamlines:
            continue

        return stream

    return streamlines[-1]


@helpers.benchmark
def read_s3fs(paths, lazy, block_size, bfile="read_file.bench"):
    fs = S3FileSystem()
    fs.invalidate_cache()

    for path in paths:
        with fs.open(path, block_size=block_size) as f:
            data = read_trk(f, lazy, bfile=bfile)


def main():
    
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    rep = int(sys.argv[3])
    nthreads = int(sys.argv[4])

    bs = 64 * 2 ** 20
    lazy = True
    header = ["vhs-bucket/hydi-header.trk"]

    helpers.drop_caches()
    fs = S3FileSystem()
    files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[start:end]
    bfile=f"../results/us-west-2-xlarge/read_s3fs_{nthreads}parallel_{start}-{end}_{rep}.csv"

    helpers.setup_bench(bfile)
    read_s3fs(files, lazy, bs, bfile=bfile)


if __name__=="__main__":
    main()
