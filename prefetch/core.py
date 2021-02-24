import os
import glob
import threading
from pathlib import Path
from shutil import disk_usage
from s3fs import S3FileSystem, S3File
from contextlib import contextmanager


class S3PrefetchFileSystem(S3FileSystem):

    default_block_size = 32 * 2 ** 20
    default_prefetch_storage = [("/dev/shm", 0)]

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
        )

        try:
            yield f
        finally:
            f.close()


class S3PrefetchFile(S3File):

    DELETE_STR = ".nibtodelete"

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
    ):


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

        # use asyncio here?
        self.fetch_thread = threading.Thread(target=self._prefetch)
        self.fetch = True
        self.fetch_thread.start()
        #self._prefetch(s3, path)

    def close(self):
        self.fetch = False
        super().close()

    # adapted from fsspec code
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

        out = self._fetch_prefetched(self.loc, self.loc + length)
        return out

    def _prefetch(self):
        """Concurrently fetch data from S3 in blocks and store in cache
        """
        fn_prefix = os.path.basename(self.path)

        # try / except as filesystem may be closed by read thread
        try:
            total_bytes = self.s3.du(self.path)
            offset = 0

            # Loop until all data has been read
            while self.fetch:
                # remove files flagged for deletion
                for c in [cache[0] for cache in self.prefetch_storage]:
                    for p in Path(c).glob(f"*{self.DELETE_STR}"):
                        p.unlink()

                # NOTE: will use a bit of memory to read/write file. Need to warn user
                # Prefetch to cache
                for path, space in self.prefetch_storage:

                    space *= 1024 ** 2  # convert to bytes from megabytes

                    if space == 0:
                        avail_cache = disk_usage(path).free
                    else:
                        avail_cache = min(disk_usage(path).free, space)

                    while self.fetch and avail_cache >= self.blocksize and total_bytes > offset:

                        data = self._fetch_range(offset, offset + self.blocksize)

                        # only write to final path when data copy is complete
                        tmp_path = os.path.join(path, f".{fn_prefix}.{offset}.tmp")
                        final_path = os.path.join(path, f"{fn_prefix}.{offset}")

                        with open(tmp_path, "wb") as f:
                            f.write(data)

                        os.rename(tmp_path, final_path)
                        offset += self.blocksize

                        if space == 0:
                            avail_cache = disk_usage(path).free
                        else:
                            avail_cache = min(disk_usage(path).free, space)

                    # if we have already read the entire file terminate prefetching
                    if total_bytes <= offset:
                        self.fetch = False
                        break
        except Exception as e:
            print(str(e))

    def _fetch_prefetched(self, start, end):
        total_read_len = end - start
        out = b""

        while len(out) < total_read_len :
            block, pos = self._get_block()

            if block is None:
                out += self._fetch_range(start, end)
                self.loc = end
                return out
            else:
                read_len = min(end, pos[1]) - block.tell() - pos[0]
                out += block.read(read_len)
                self.loc = start + read_len
                start = self.loc 

                if start > end:
                    os.rename(block.name, f"{block.name}{self.DELETE_STR}")
                block.close()

        return out


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

        fn_prefix = os.path.basename(self.path)
        pf_dirs = [p[0] for p in self.prefetch_storage ]
        cached_files = [
            fn
            for fs in pf_dirs
            for fn in glob.glob(os.path.join(fs, f"{fn_prefix}.*[0-9]"))
        ]

        # Iterate through the cached files/offsets
        for f in cached_files:

            # Get position of cached block relative to original file
            b_start = int(f.split(".")[-1])
            b_end = b_start + os.stat(f).st_size

            if self.loc >= b_start and self.loc < b_end:
                cf_ = open(f, "rb")
                c_offset = self.loc - b_start
                cf_.seek(c_offset, os.SEEK_SET)
                return cf_, (b_start, b_end)

        return None, None
