import sys

from nibabel.streamlines import TrkFile
from prefetch.core import S3PrefetchFileSystem

import helpers


def read_trk(f, lazy, bfile="read_file.bench"):
    streamlines = TrkFile.load(f, lazy_load=lazy).streamlines

    if lazy:
        for stream in streamlines:
            continue

        return stream

    return streamlines[-1]


@helpers.benchmark
def read_prefetched(path, lazy, block_size, prefetch_storage, bfile="read_file.bench", header_bytes=1000):
    helpers.drop_caches()
    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    with fs.open(path, block_size=block_size, prefetch_storage=prefetch_storage, header_bytes=header_bytes) as f:
        data = read_trk(f, lazy, bfile=bfile)

    print(data)


def main():
    
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    rep = int(sys.argv[3])
    nthreads = int(sys.argv[4])

    prefetch_storage = [(f'/dev/shm/{end//(end-start)}', 1*1024)]
    bs = 64 * 2 ** 20
    lazy = True
    header = ["vhs-bucket/hydi-header.trk"]

    fs = S3PrefetchFileSystem()
    files = fs.glob("hydi-tractography/hydi_tracks.*.trk")[start:end]
    bfile=f"../results/us-west-2-xlarge/read_prefetch_{nthreads}parallel_{start}-{end}_{rep}.csv"

    helpers.setup_bench(bfile)
    read_prefetched(header+files, lazy, bs, prefetch_storage, bfile=bfile)


if __name__=="__main__":
    main()
