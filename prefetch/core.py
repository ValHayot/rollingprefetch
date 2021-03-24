import os
import asyncio
import threading
import concurrent.futures
import multiprocessing as mp
from time import sleep
from copy import deepcopy
from pathlib import Path
from shutil import disk_usage
from s3fs import S3FileSystem, S3File
from s3fs.core import _fetch_range
from contextlib import contextmanager

import logging
import logging.config


class S3PrefetchFileSystem(S3FileSystem):

    default_block_size = 32 * 2 ** 20
    default_prefetch_storage = [("/dev/shm", 0)]

    # init debugger
    logfile = "logging.conf"
    logger = logging.getLogger(__name__)

    try:
        logging.config.fileConfig(logfile)
    except Exception as e:
        logger.setLevel(logging.ERROR)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(ch)

    def __init__(self, default_block_size=None, **kwargs):

        super().__init__(**kwargs)

        self.default_block_size = default_block_size or self.default_block_size
        self.logger.info(
            "Initializing S3PrefetchFileSystem with default_block_size %d",
            self.default_block_size,
        )

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

        self.logger.debug("Call to S3PrefetchFileSystem _open")

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

    # def _ls_from_cache(self, path):
    #    return None


class S3PrefetchFile(S3File):

    DELETE_STR = ".nibtodelete"

    # @profile
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

        self.s3.logger.info("Opening S3Prefetch file")

        self.prefetch_storage = []

        self.prefetch_storage = prefetch_storage
        self.header_bytes = header_bytes
        self.path_sizes = [self.s3.du(p) for p in self.file_list]
        self.file_idx = 0

        self.size = sum(
            self.path_sizes[i] - self.header_bytes for i in range(len(self.path_sizes))
        )

        self.fetch = True

        self.s3.logger.debug("Lauching prefetch thread")
        self.fetch_thread = threading.Thread(
            target=self._prefetch,
            args=(
                deepcopy(self.file_list),
                deepcopy(self.prefetch_storage),
                deepcopy(self.path_sizes),
                self.blocksize,
                deepcopy(self.req_kw),
            ),
        )

        self.fetch_thread.start()

        self.s3.logger.debug("Lauching evict thread")
        self.evict_thread = threading.Thread(
            target=self._remove,
            args=(
                deepcopy(self.prefetch_storage),
                deepcopy(self.path_sizes),
                self.blocksize,
                deepcopy(self.file_list),
                self.DELETE_STR,
            ),
        )
        self.evict_thread.start()

        self.b_start = 0
        self.b_end = self.blocksize
        self.cf_ = None
        self.s3.logger.debug("S3PrefetchFile initialization complete")

    async def main(self):
        loop = asyncio.get_running_loop()

        with concurrent.futures.ProcessPoolExecutor() as pool:
            await loop.run_in_executor(
                pool,
                self._prefetch,
                *(
                    deepcopy(self.s3),
                    deepcopy(self.fs),
                    deepcopy(self.file_list),
                    deepcopy(self.prefetch_storage),
                    deepcopy(self.path_sizes),
                    self.blocksize,
                    deepcopy(self.req_kw),
                ),
            )

            await loop.run_in_executor(
                pool, self._remove, deepcopy(self.prefetch_storage), self.DELETE_STR
            )

    # @profile
    def close(self):
        self.s3.logger.debug("Closing S3PrefetchFile")
        self.fetch = False
        super().close()

    def seek(self, pos, whence=0):
        try:
            # need to fix
            if whence == 0:
                p = pos - self.b_start
            else:
                p = pos
            self.s3.logger.debug(
                "Seeking to position %d relative to %d in file %s",
                p,
                whence,
                self.cf_.name,
            )
            self.cf_.seek(p, whence)
        except Exception as e:
            self.s3.logger.error(
                "Failed to seek in cached block from %d relative to %d with error message %s",
                pos,
                whence,
                str(e),
            )
        super().seek(pos, whence)

    # adapted from fsspec code
    # @profile
    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        Parameters
        ----------
        length: int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        self.s3.logger.debug(
            "Reading the next %d bytes from file %s", length, self.path
        )
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

    def _remove(self, prefetch_storage, path_sizes, blocksize, file_list, DELETE_STR):
        self.s3.logger.debug("Removing files in cache with extension %s", DELETE_STR)

        pf_dirs = [p[0] for p in prefetch_storage]
        file_keys = [f.split("/")[1] for f in file_list]
        block_ids = [
            f"{file_keys[i]}.{int(j*blocksize)}{DELETE_STR}"
            for i in range(len(file_keys))
            for j in range(int(path_sizes[i] // blocksize) + 1)
        ]

        found = False
        updated_list = deepcopy(block_ids)
        idx = None
        while self.fetch:
            for fname in block_ids:
                for pf in pf_dirs:
                    try:
                        os.remove(os.path.join(pf, fname))
                        idx = updated_list.index(fname)
                        updated_list.remove(fname)
                        found = True
                        self.s3.logger.debug("Removed %s", fname)
                        break
                    except Exception as e:
                        found = False

            block_ids = deepcopy(updated_list)
            sleep(20)
        self.s3.logger.debug("Removal complete")

    # @profile
    def _prefetch(self, file_list, prefetch_storage, path_sizes, blocksize, req_kw):
        """Concurrently fetch data from S3 in blocks and store in cache"""

        fs = S3FileSystem()
        s3 = fs

        offset = 0
        file_idx = 0
        total_bytes = path_sizes[file_idx]
        total_files = len(file_list)

        prefetch_space = {
            path: {"total": space * 1024 ** 2, "used": 0}
            for path, space in prefetch_storage
        }
        fetched_paths = []

        # Loop until all data has been read
        self.s3.logger.debug("Prefetching started")
        while self.fetch:
            # NOTE: will use a bit of memory to read/write file. Need to warn user
            # Prefetch to cache
            for path, space in prefetch_storage:
                if space == 0:
                    avail_cache = disk_usage(path).free
                    prefetch_space[path]["total"] = avail_cache

                # Repeating the fetch to avoid infinite loop
                while self.fetch and total_bytes > offset:
                    # try / except as filesystem may be closed by read thread
                    try:

                        avail_space = (
                            prefetch_space[path]["total"] - prefetch_space[path]["used"]
                        )
                        if avail_space < blocksize:
                            if len(fetched_paths) > 0:
                                for i in range(len(fetched_paths)):
                                    if os.path.exists(fetched_paths[i]):
                                        break
                                    elif path in fetched_paths[i]:
                                        prefetch_space[path]["used"] -= blocksize
                                        avail_space += blocksize
                                        self.s3.logger.warning(
                                            "Path %s has been evicted. Used space on %s now %d/%d",
                                            fetched_paths[i],
                                            path,
                                            prefetch_space[path]["used"],
                                            prefetch_space[path]["total"],
                                        )

                                fetched_paths = fetched_paths[i:]

                        bucket, key, version_id = s3.split_path(file_list[file_idx])
                        data = _fetch_range(
                            fs,
                            bucket,
                            key,
                            version_id,
                            offset,
                            offset + blocksize,
                            req_kw=req_kw,
                        )

                        # only write to final path when data copy is complete
                        tmp_path = os.path.join(path, f".{key}.{offset}.tmp")
                        final_path = os.path.join(path, f"{key}.{offset}")
                        self.s3.logger.debug("Prefetched data to %s", final_path)

                        with open(tmp_path, "wb") as f:
                            f.write(data)

                        prefetch_space[path]["used"] += blocksize

                        os.rename(tmp_path, final_path)
                        fetched_paths.append(final_path)

                        offset += int(blocksize)

                    except Exception as e:
                        self.s3.logger.error(
                            "An error occured during prefetch process: %s", str(e)
                        )

                    # if we have already read the entire file terminate prefetching
                    # can use walrus op here
                    if total_bytes <= offset and file_idx + 1 < total_files:
                        self.s3.logger.debug(
                            "Prefetched all of file %s data. Moving onto next file %s",
                            file_list[file_idx],
                            file_list[file_idx + 1],
                        )
                        file_idx += 1
                        total_bytes = path_sizes[file_idx]
                        offset = 0
                    elif total_bytes <= offset:
                        self.fetch = False
                        self.s3.logger.debug("Prefetched all bytes")
                        break

    # @profile
    def _fetch_prefetched(self, start, end):
        total_read_len = end - start
        out = b""

        while len(out) < total_read_len:
            block, pos = self._get_block()
            # if block is not None:
            #    try:
            #        block.seek(start - self.b_start, 0)
            #    except Exception as e:
            #        print("could not seek", str(e))
            #        pass

            if block is None:
                # if self.loc > 905969164:
                #    print("block is none", self.loc, start, end)
                # EOF error
                try:
                    if end < start:
                        end += start
                    self.s3.logger.debug(
                        "Fetching data from position %d to %d directly from S3",
                        start,
                        end,
                    )
                    out += self._fetch_range(start, end)
                except Exception as e:
                    # if self.loc > 905969164:
                    #    print("fetch_range_error", str(e))
                    self.s3.logger.error(
                        "Error with fetching data from S3 at position %d - %d: %s",
                        start,
                        end,
                        str(e),
                    )
                    self.loc += len(out)
                    return out

                self.loc += end - start
                start = self.loc
            else:

                curr_pos = block.tell()
                read_len = int(min(end, pos[1]) - curr_pos - pos[0])
                self.s3.logger.debug(
                    "Reading data from cached block %s in range [%d, %d]",
                    block.name,
                    curr_pos,
                    curr_pos + read_len,
                )
                # if self.loc > 905969164:
                #    print("seek loc", start)
                #    print(end, pos, block.tell(), read_len)
                out += block.read(read_len)
                # if self.loc > 905969164:
                #    print("out", len(out), total_read_len)
                self.loc += read_len
                start = self.loc

                if start >= pos[1]:
                    self.s3.logger.debug(
                        "Block %s read entirely (current position %d). Flagging for deletion",
                        block.name,
                        self.loc,
                    )
                    block.close()
                    os.rename(block.name, f"{block.name}{self.DELETE_STR}")

            if start >= self.path_sizes[self.file_idx] and self.file_idx + 1 < len(
                self.file_list
            ):
                self.s3.logger.debug(
                    "Current block %s read entirely. Loading new block %s at position %d",
                    self.path,
                    self.file_list[self.file_idx + 1],
                    self.header_bytes,
                )
                self.file_idx += 1
                self.path = self.file_list[self.file_idx]
                self.bucket, self.key, self.version_id = self.s3.split_path(self.path)
                self.path_size = self.path_sizes[self.file_idx]
                self.loc = self.header_bytes
                start = self.loc
                end = total_read_len - len(out) + self.loc

        # print(total_read_len, len(out))

        return out

    # @profile
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

        # fn_prefix = os.path.basename(self.path)

        pf_dirs = [p[0] for p in self.prefetch_storage]
        bid = [
            i * int(self.blocksize)
            for i in range(self.path_sizes[self.file_idx] + 1)
            if self.loc - self.blocksize <= i * self.blocksize
        ][0]
        cached_files = [os.path.join(fs, f"{self.key}.{bid}") for fs in pf_dirs]

        if self.cf_ is not None and self.loc >= self.b_start and self.loc < self.b_end:
            # self.cf_.seek
            self.s3.logger.debug(
                "Position %d is found in currently loaded file %s which spans ranges [%d, %d]",
                self.loc,
                self.cf_.name,
                self.b_start,
                self.b_end,
            )
            return self.cf_, (self.b_start, self.b_end)
        try:
            self.cf_.close()
        except Exception as e:
            self.s3.logger.warning("Was not able to close block. exception: %s", str(e))

        # Iterate through the cached files/offsets
        for f in cached_files:

            try:
                while os.path.exists(f".{f}.tmp"):
                    sleep(0.005)
                # Get position of cached block relative to original file
                b_start = int(f.split(".")[-1])
                b_end = min(b_start + os.path.getsize(f), self.size)
                # if self.loc > 905969164:
                #    print("curr file", f, b_start, b_end, self.loc)
                self.s3.logger.debug(
                    "Position %d found in block %s with range [%d, %d]",
                    self.loc,
                    f,
                    b_start,
                    b_end,
                )

                self.cf_ = open(f, "rb")
                self.b_start = b_start
                self.b_end = b_end
                c_offset = self.loc - self.b_start
                self.cf_.seek(c_offset, os.SEEK_SET)
                return self.cf_, (self.b_start, self.b_end)
            except Exception as e:
                self.s3.logger.warning(
                    "Exception occured while opening block at position %d: %s",
                    self.loc,
                    str(e),
                )
                self.cf_ = None
                self.b_start = None
                self.b_end = None

        self.s3.logger.error("Position %d not found in any cached block", self.loc)

        return None, None
