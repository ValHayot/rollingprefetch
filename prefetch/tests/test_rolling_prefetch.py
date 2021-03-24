#!/usr/bin/env python
import os
import threading
import pytest
from threading import Thread

import s3fs
from moto import mock_s3

from time import sleep
from pathlib import Path

from s3fs.core import S3FileSystem
from ..core import S3PrefetchFileSystem, S3PrefetchFile


CACHE_DIR = "/dev/shm"
CACHE_SIZE = 1024 ** 2
CACHES = {CACHE_DIR: CACHE_SIZE}
DELETE_STR = ".nibtodelete"
BUCKET_NAME = "s3trk"
BLOCK_SIZE = CACHE_SIZE // 4

port = 5555
endpoint_uri = "http://127.0.0.1:%s/" % port

# taken from s3fs tests https://github.com/dask/s3fs/blob/main/s3fs/tests/test_s3fs.py#L57
@pytest.fixture()
def s3_base():
    # writable local S3 system
    import shlex
    import subprocess
    import requests
    import time

    proc = subprocess.Popen(shlex.split("moto_server s3 -p %s" % port))

    timeout = 5
    while timeout > 0:
        try:
            r = requests.get(endpoint_uri)
            if r.ok:
                break
        except:
            pass
        timeout -= 0.1
        time.sleep(0.1)
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture()
def s3(s3_base):
    from botocore.session import Session

    # NB: we use the sync botocore client for setup
    session = Session()
    client = session.create_client("s3", endpoint_url=endpoint_uri)
    client.create_bucket(Bucket=BUCKET_NAME)

    yield


@pytest.fixture
def create_main_file(s3):
    # create main file
    fname = "random.bin"

    # generate a random bytestring
    data = os.urandom(CACHE_SIZE)

    s3_path = os.path.join(BUCKET_NAME, fname)
    fs = s3fs.S3FileSystem()

    with fs.open(s3_path, "wb") as f:
        f.write(data)

    return s3_path


@pytest.fixture
def create_multi_files(s3):
    s3_paths = [os.path.join(BUCKET_NAME, f"random_{i}.bin") for i in range(4)]

    fs = s3fs.S3FileSystem()
    for p in s3_paths:
        with fs.open(p, "wb") as f:
            f.write(os.urandom(int(BLOCK_SIZE * 2)))

    return s3_paths


@pytest.fixture
def create_cached(create_main_file):

    s3_path = str(create_main_file)
    fname = os.path.basename(s3_path)

    # take a chunk and save it to tmpfs space
    fs = s3fs.S3FileSystem()
    f = fs.open(s3_path, "rb")

    csize = BLOCK_SIZE
    cfname_0 = os.path.join(CACHE_DIR, f"{fname}.0")
    cfname_1 = os.path.join(CACHE_DIR, f"{fname}.{csize}")

    with open(cfname_0, "wb") as cf_:
        cf_.write(f.read(csize))

    with open(cfname_1, "wb") as cf_:
        cf_.write(f.read(csize))

    f.seek(0, os.SEEK_SET)

    cf_ = open(cfname_0, "rb")

    return {
        "f": f,
        "nbytes": csize * 2 + 256,
        "fn_prefix": fname,
        "cf_": cf_,
        "fidx": (0, csize),
        "s3_path": s3_path,
    }


def cleanup(fn_prefix):
    for c in Path(CACHE_DIR).glob(fn_prefix + "*"):
        c.unlink()


def _skip_init(cls):
    init = cls.__init__
    cls.__init__ = lambda *args, **kwargs: None
    instance = cls()
    cls.__init__ = init
    return instance


def test_prefetch(create_main_file):
    fname = create_main_file

    fs = S3PrefetchFileSystem()

    s3pf = _skip_init(S3PrefetchFile)
    s3pf.s3 = fs
    s3pf.fetch = True
    s3pf._prefetch([fname], list(CACHES.items()), [CACHE_SIZE], BLOCK_SIZE, fs.req_kw)

    f_bn = os.path.basename(fname)

    cached_files = Path(CACHE_DIR).glob(f"{f_bn}*")
    cf = list(cached_files)
    assert len(cf) == 4


def evict_timeout(s):
    sleep(20)
    s.fetch = False


def test_eviction(create_main_file):
    fname = create_main_file
    f_bn = os.path.basename(fname)

    for c in Path(CACHE_DIR).glob(f_bn + "*"):

        if DELETE_STR not in c.name:
            c.rename(os.path.join(CACHE_DIR, f"{c.name}{DELETE_STR}"))

    fs = S3PrefetchFileSystem()

    s3pf = _skip_init(S3PrefetchFile)
    s3pf.s3 = fs
    s3pf.fetch = True
    s3pf.loc = 0
    s3pf.blocksize = BLOCK_SIZE

    # to enable some eviction
    t = threading.Thread(target=evict_timeout, args=[s3pf])
    t.start()

    s3pf._remove(list(CACHES.items()), [CACHE_SIZE], BLOCK_SIZE, [fname], DELETE_STR)

    cached_files = Path(CACHE_DIR).glob(f"{f_bn}*")
    cf = list(cached_files)
    assert len(cf) == 0


def test_get_block(create_cached):

    cc = dict(create_cached)

    fs = S3PrefetchFileSystem()
    with fs.open(
        cc["s3_path"],
        "rb",
        block_size=BLOCK_SIZE,
        prefetch_storage=list(CACHES.items()),
    ) as f:
        cf_, offset = f._get_block()

    print(cc)
    assert cf_.name == cc["cf_"].name
    assert offset == cc["fidx"]
    cleanup(os.path.basename(cc["s3_path"]))


def test_fetch_prefetched(create_cached):
    fs = S3PrefetchFileSystem()
    cc = dict(create_cached)

    with fs.open(
        cc["s3_path"],
        "rb",
        block_size=BLOCK_SIZE,
        prefetch_storage=list(CACHES.items()),
    ) as f:
        data = f._fetch_prefetched(0, cc["nbytes"])

    actual = cc["f"].read(cc["nbytes"])

    assert len(data) == len(actual) == cc["nbytes"]
    assert data == actual
    assert os.path.exists(f"{cc['cf_'].name}{DELETE_STR}")
    cleanup(os.path.basename(cc["s3_path"]))


def test_read_cached(create_cached):
    fs = S3PrefetchFileSystem()
    cc = dict(create_cached)

    with fs.open(
        cc["s3_path"],
        "rb",
        block_size=BLOCK_SIZE,
        prefetch_storage=list(CACHES.items()),
    ) as f:
        data = f.read(cc["nbytes"])

    assert data == cc["f"].read(cc["nbytes"])
    assert os.path.exists(f"{cc['cf_'].name}{DELETE_STR}")
    cleanup(os.path.basename(cc["s3_path"]))


def test_read_uncached(create_main_file):
    fs = S3PrefetchFileSystem()
    s3_path = str(create_main_file)

    with fs.open(
        s3_path, "rb", block_size=BLOCK_SIZE, prefetch_storage=list(CACHES.items())
    ) as f:
        data = f.read()

    fs = S3FileSystem()
    with fs.open(s3_path, "rb") as f:
        actual_data = f.read()

    assert data == actual_data
    cleanup(os.path.basename(s3_path))


def test_multi_files(create_multi_files):
    fs = S3PrefetchFileSystem()
    s3 = S3FileSystem()
    s3_paths = list(create_multi_files)

    fs.invalidate_cache()

    with fs.open(
        s3_paths,
        "rb",
        header_bytes=0,
        block_size=BLOCK_SIZE,
        prefetch_storage=list(CACHES.items()),
    ) as f:
        for p in s3_paths:
            assert f.path == p
            data = f.read(BLOCK_SIZE * 2)
            s3file = s3.open(p, "rb")
            actual = s3file.read()

            assert len(data) == len(actual)
            assert data == actual

            s3file.close()

    cleanup(os.path.basename("random"))
