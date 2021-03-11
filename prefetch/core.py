import os
import glob
import asyncio
import threading
import concurrent.futures
import multiprocessing as mp
from copy import deepcopy
from pathlib import Path
from shutil import disk_usage
from s3fs import S3FileSystem, S3File
from s3fs.core import _fetch_range
from contextlib import contextmanager


class S3PrefetchFileSystem(S3FileSystem):

    default_block_size = 32 * 2 ** 20
    default_prefetch_storage = [("/dev/shm", 0)]

    #@profile
    def __init__(self, default_block_size=None, **kwargs):
        super().__init__(**kwargs)

        self.default_block_size = default_block_size or self.default_block_size

    # much of this function consists of what's done in s3fs
    # main differences are setting the cache to None
    # and prefetch_storage (might switch to cache_storage later??)
    # for now forcing mode to be "rb" TODO change?
    @contextmanager
    def _open(
        self,
        path,
        block_size=None,
        acl="",
        version_id=None,
        prefetch_storage=None,
        autocommit=True,
        requester_pays=None,
        header_bytes=0,
        **kwargs,
    ):
        # path can be a list of files
        # caching turned off as prefetch fs
        # prefetch_path passed as cached_storage
        # caching is by default set to none
        if block_size is None:
            block_size = self.default_block_size
        if requester_pays is None:
            requester_pays = bool(self.req_kw)
        if prefetch_storage is None:
            prefetch_storage = self.default_prefetch_storage

        acl = acl or self.s3_additional_kwargs.get("ACL", "")
        kw = self.s3_additional_kwargs.copy()
        kw.update(kwargs)
        if not self.version_aware and version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )
        fill_cache = False
        cache_type = "none"
        mode = "rb"
        self.get_object = S3PrefetchFileSystem

        f = S3PrefetchFile(
            self,
            path,
            mode,
            prefetch_storage=prefetch_storage,
            block_size=block_size,
            acl=acl,
            version_id=version_id,
            fill_cache=fill_cache,
            s3_additional_kwargs=kw,
            cache_type=cache_type,
            autocommit=autocommit,
            requester_pays=requester_pays,
            header_bytes=header_bytes,
        )

        try:
            yield f
        finally:
            f.close()

    #def _ls_from_cache(self, path):
    #    return None


class S3PrefetchFile(S3File):

    DELETE_STR = ".nibtodelete"

    #@profile
    def __init__(
        self,
        s3,
        path,
        mode="rb",
        prefetch_storage=None,
        block_size=5 * 2 ** 20,
        acl="",
        version_id=None,
        fill_cache=False,
        s3_additional_kwargs=None,
        autocommit=True,
        cache_type="none",
        requester_pays=False,
        header_bytes=0,
    ):

        if isinstance(path, list):
            self.file_list = path
            path = path[0]
        else:
            self.file_list = [path]

        super().__init__(
            s3,
            path,
            mode,
            block_size=block_size,
            acl=acl,
            version_id=version_id,
            fill_cache=fill_cache,
            s3_additional_kwargs=s3_additional_kwargs,
            autocommit=autocommit,
            cache_type=cache_type,
            requester_pays=requester_pays,
        )

        self.prefetch_storage = []

        self.prefetch_storage = prefetch_storage
        self.header_bytes = header_bytes
        self.path_sizes = [self.s3.du(p) for p in self.file_list]
        self.file_idx = 0

        self.size = sum(self.path_sizes[i] - self.header_bytes for i in range(len(self.path_sizes)))
        print("file: ", self.path)

        self.fetch = True
        #asyncio.run(self.main())
        print("after asyncio run")
        # use asyncio here?
        self.fetch_thread = mp.Process(target=self._prefetch,
                args=(deepcopy(self.file_list), 
                      deepcopy(self.prefetch_storage),
                      deepcopy(self.path_sizes),
                      self.blocksize,
                      self.DELETE_STR,
                      deepcopy(self.req_kw)))
        self.fetch_thread.start()
        self.b_start = 0
        self.b_end = self.blocksize
        self.cf_ = None
        #self._prefetch(s3, path)

    async def main(self):
        loop = asyncio.get_running_loop()

        with concurrent.futures.ProcessPoolExecutor() as pool:
            await loop.run_in_executor(pool, self._prefetch,
                    *(deepcopy(self.s3),
                      deepcopy(self.fs),
                      deepcopy(self.file_list), 
                      deepcopy(self.prefetch_storage),
                      deepcopy(self.path_sizes),
                      self.blocksize,
                      self.DELETE_STR,
                      deepcopy(self.req_kw)))

            await loop.run_in_executor(pool, self._remove, deepcopy(self.prefetch_storage), self.DELETE_STR)

    #@profile
    def close(self):
        self.fetch = False
        super().close()

    def seek(self, pos, whence=0):
        try:
            #need to fix
            if whence == 0:
                p = pos - self.b_start
            else:
                p = pos
            self.cf_.seek(pos, whence)
        except Exception as e:
            print("seek error", print(e), self.cf_, pos, whence)
        super().seek(pos, whence)
        

    # adapted from fsspec code
    #@profile
    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        Parameters
        ----------
        length: int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        length = -1 if length is None else int(length)
        if length < 0:
            length = self.size - self.loc
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if length == 0:
            # don't even bother calling fetch
            return b""

        #import sys
        #if self.loc / self.blocksize > 1:
        #    sys.exit(1)

        out = self._fetch_prefetched(self.loc, self.loc + length)

        return out

    def _remove(self, prefetch_storage, DELETE_STR):
        while self.fetch:
            for c in [cache[0] for cache in prefetch_storage]:
                for p in Path(c).glob(f"*{DELETE_STR}"):
                    p.unlink()
        print("done remove")

    #@profile
    def _prefetch(self, file_list, prefetch_storage, path_sizes, blocksize, DELETE_STR, req_kw):
        """Concurrently fetch data from S3 in blocks and store in cache
        """
        print("procees")

        fetch = True
        fs = S3FileSystem()
        s3 = fs

        # try / except as filesystem may be closed by read thread
        try:
            offset = 0
            file_idx = 0
            total_bytes = path_sizes[file_idx]
            total_files = len(file_list)

            prefetch_space = { path: { "total": space * 1024 ** 2, "used": 0 } for path, space in prefetch_storage }

            # Loop until all data has been read
            while fetch:
                # remove files flagged for deletion
                #for c in [cache[0] for cache in prefetch_storage]:
                #    for p in Path(c).glob(f"*{DELETE_STR}"):
                #        p.unlink()

                # NOTE: will use a bit of memory to read/write file. Need to warn user
                # Prefetch to cache
                for path, space in prefetch_storage:
                    if space == 0:
                        avail_cache = disk_usage(path).free
                        prefetch_space[path]["total"] = avail_cache

                    while fetch and total_bytes > offset:

                        avail_space = prefetch_space[path]["total"] - prefetch_space[path]["used"]
                        if avail_space < blocksize:
                            break

                        bucket, key, version_id  = s3.split_path(file_list[file_idx])
                        data = _fetch_range(fs, bucket, key, version_id, offset, offset + blocksize, req_kw=req_kw,)

                        # only write to final path when data copy is complete
                        tmp_path = os.path.join(path, f".{key}.{offset}.tmp")
                        final_path = os.path.join(path, f"{key}.{offset}")

                        with open(tmp_path, "wb") as f:
                            f.write(data)

                        prefetch_space[path]["used"] += blocksize

                        os.rename(tmp_path, final_path)
                        offset += blocksize

                    # if we have already read the entire file terminate prefetching
                    # can use walrus op here
                    if total_bytes <= offset and file_idx + 1 < total_files:
                        file_idx += 1
                        total_bytes = path_sizes[file_idx]
                        #fn_prefix = os.path.basename(file_list[file_idx])
                        offset = 0
                    elif total_bytes <= offset:
                        fetch = False
                        print("Prefetched all files")
                        break

        except Exception as e:
            print("Prefetch error: ", str(e))

    #@profile
    def _fetch_prefetched(self, start, end):
        total_read_len = end - start
        out = b""

        while len(out) < total_read_len :
            block, pos = self._get_block()
            #if block is not None:
            #    try:
            #        block.seek(start - self.b_start, 0)
            #    except Exception as e:
            #        print("could not seek", str(e))
            #        pass

            if block is None:
                #if self.loc > 905969164:
                #    print("block is none", self.loc, start, end)
                # EOF error
                try:
                    out += self._fetch_range(start, end)
                except Exception as e:
                    #if self.loc > 905969164:
                    #    print("fetch_range_error", str(e))
                    self.loc += len(out)
                    return out

                self.loc += (end - start)
                start = self.loc
            else:

                read_len = min(end, pos[1]) - block.tell() - pos[0]
                #if self.loc > 905969164:
                #    print("seek loc", start)
                #    print(end, pos, block.tell(), read_len)
                out += block.read(read_len)
                #if self.loc > 905969164:
                #    print("out", len(out), total_read_len)
                self.loc += read_len
                start = self.loc 

                if start >= pos[1]:
                    os.rename(block.name, f"{block.name}{self.DELETE_STR}")

            if start >= self.path_sizes[self.file_idx] and self.file_idx + 1 < len(self.file_list):
                self.file_idx += 1
                self.path = self.file_list[self.file_idx]
                self.bucket, self.key, self.version_id = self.s3.split_path(self.path)
                self.path_size = self.path_sizes[self.file_idx]
                self.loc = self.header_bytes
                start = self.loc
                end = total_read_len - len(out) + self.loc

        #print(total_read_len, len(out))

        return out


    #@profile
    def _get_block(self):
        """Open the cached block fileobj at the necessary file offset
        Returns
        -------
        cf_ : fileobj
            Fileobj of the cached file (returns None if it does not exist)
        k : tuple (int, int)
            The positioning of the opened block respective to the original file
        """
        # Get the list of files in cache

        #fn_prefix = os.path.basename(self.path)
     
        pf_dirs = [p[0] for p in self.prefetch_storage ]
        block_ids = [i*self.blocksize for i in range(int(self.path_sizes[self.file_idx]/self.blocksize)+1)]
        cached_files = [os.path.join(fs, f"{self.key}.{bid}") for fs in pf_dirs for bid in block_ids]

        #cached_files = [
        #    fn
        #    for fs in pf_dirs
        #    for fn in glob.glob(os.path.join(fs, f"{self.key}.*[0-9]"))
        #]

        # Iterate through the cached files/offsets
        if self.cf_ is not None and self.loc >= self.b_start and self.loc < self.b_end:
            #self.cf_.seek
            return self.cf_, (self.b_start, self.b_end)
        try:
            self.cf_.close()
        except Exception as e:
            print(str(e))

        try:
            for f in cached_files:

                # Get position of cached block relative to original file
                b_start = int(f.split(".")[-1])
                b_end = min(b_start + self.blocksize, self.size)
                #if self.loc > 905969164:
                #    print("curr file", f, b_start, b_end, self.loc)

                
                if self.loc >= b_start and self.loc < b_end:
                    #print(f)
                    self.cf_ = open(f, "rb")
                    self.b_start = b_start
                    self.b_end = b_end
                    c_offset = self.loc - self.b_start
                    self.cf_.seek(c_offset, os.SEEK_SET)
                    return self.cf_, (self.b_start, self.b_end)
        except Exception as e:
            print("**open block error**", str(e))
            self.cf_ = None
            self.b_start = None
            self.b_end = None


        return None, None


