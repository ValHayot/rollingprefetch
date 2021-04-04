#!/usr/bin/env python
import inspect
import subprocess as sp
from os import getpid
from time import perf_counter_ns
from functools import wraps
from nibabel.streamlines import TrkFile


def setup_bench(file=None):
    try:
        with open(file, "w+") as f:
            f.write("action,file,timestamp,runtime,lazy,blocksize,prefetch_storage\n")
    except Exception as e:
        out_string = f"""
                     Action\t\tFile\t\tTimestamp\t\tRuntime\t\tLazy\t\tBlocksize\t\tPrefetch
                     {'-'*100}
                     """
        print(inspect.cleandoc(out_string))


# decorator
def benchmark(func):
    @wraps(func)
    def _benchmark(*args, **kwargs):
        start = perf_counter_ns()

        largs = list(args)

        if len(largs) == 2:
            largs.append(0)
        if len(largs) == 3:
            largs.append([])

        try:
            return func(*args, **kwargs)
        finally:
            end = perf_counter_ns()
            try:
                with open(kwargs["bfile"], "a+") as f:
                    f.write(
                        f"{func.__name__},\"{largs[0]}\",{start},{(end-start)*10**-9},{largs[1]},{largs[2]},\"{largs[3]}\"\n"
                    )
            except Exception as e:
                out_string = f"""
                {func.__name__}\t"{largs[0]}"\t{start}\t{(end-start)*10**-9}\t{largs[1]}\t{largs[2]}\t"{largs[3]}"
                """
                print(inspect.cleandoc(out_string))

    return _benchmark


def drop_caches():
    print("** DROPPING CACHES **")
    out = sp.run(
        "echo 3 | sudo tee /proc/sys/vm/drop_caches", capture_output=True, shell=True
    )
    print("STDOUT: ", out.stdout.decode("UTF-8"), end="")
    print("STDERR: ", out.stderr.decode("UTF-8"))
    print("** DROPPING CACHES COMPLETED **")


def read_trk(f, lazy, bfile="read_file.bench"):
    streamlines = TrkFile.load(f, lazy_load=lazy).streamlines

    if lazy:
        for stream in streamlines:
            continue