from prefetch.core import S3PrefetchFileSystem
from s3fs import S3FileSystem

from nibabel.streamlines import TrkFile

from time import perf_counter_ns


def write_benchmark(b_file, function, paths, start, end):
    with open(b_file, "a+") as f:
        f.write(f"{function},{paths},{start},{end}\n")
    

def prefetched_read(paths, benchmark_file, block_size=64 * 2**20, prefetch_storage=[("/dev/shm", 5*1024), (".", 6*1024)]):
    # insert header file
    paths.insert(0, "vhs-testbucket/hydi-header.trk")

    start = perf_counter_ns()
    fs = S3PrefetchFileSystem()
    fs.invalidate_cache()

    with fs.open(paths, header_bytes=1000, block_size=block_size, prefetch_storage=prefetch_storage) as f:
        streamlines = TrkFile.load(f, lazy_load=True).streamlines

        for stream in streamlines:
            continue

    end = perf_counter_ns()

    write_benchmark(benchmark_file, "prefetch", paths, start, end)

    print(stream)


def reg_read(paths, benchmark_file, block_size=64 * 2**20):
    # insert header file
    paths.insert(0, "vhs-testbucket/hydi-header.trk")

    start = perf_counter_ns()
    fs = S3FileSystem()
    fs.invalidate_cache()

    for p in paths:
        with fs.open(p, block_size=block_size) as f:
            streamlines = TrkFile.load(f, lazy_load=True).streamlines

            for stream in streamlines:
                continue

    end = perf_counter_ns()

    write_benchmark(benchmark_file, "s3fs", paths, start, end)

    print(stream)

def main():
    paths = S3FileSystem().glob("hydi-tractography/hydi*")
    benchmark_file = "bench_all.out"

    prefetched_read(paths, benchmark_file)
    reg_read(paths, benchmark_file)
    
    print(paths)

if __name__=="__main__":
    main()





