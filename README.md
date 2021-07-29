# Rolling Prefetch


Rolling Prefetch is an API extension to [S3Fs](https://github.com/dask/s3fs) that enables the prefetching of sequentially read files
while evicting those that have been previously read.
It was inspired by the need to read a large tractography file (~500GB) split up into multiple blocks from S3.

## How it works
The Rolling Prefetch API is similar to that of S3Fs with some slight modifications.
To create a Rolling Prefetch instance, it is necessary to first create a `S3PrefetchFileSystem` instance. 
e.g.
```
from prefetch import S3PrefetchFileSystem

fs = S3PrefetchFileSystem()
```

The `S3PrefetchFilesystem` behaves identically to `S3FileSystem` except that it creates a read-only filesystem and sets S3Fs caching strategies
to `None`.

Once the `S3PrefetchFileSystem` is generated, it is now possible to specify which files need to be preloaded in what order, which filesystems to use a cache, and how to read the files.

For instance, to read a single file
```
from prefetch import S3PrefetchFileSystem

fs = S3PrefetchFileSystem()

block_size = 32*2**20
prefetch_storage = [("/tmp", 5*1024)]
path = "somefile.txt"
    
with fs.open(path, block_size=block_size, prefetch_storage=prefetch_storage) as f:
  # do something with file
```
Here we specified `block_size`, `prefetch_storage` alongside the path to read.
`block_size` is the same parameter that is found in S3Fs. It denotes how big the read chunks should be in bytes. The default is 32MB.
`prefetch_storage` is a list of tuples in order of descending priority. Each tuple consists of a directory path where to cache too, and how much
prefetch space is allocated to that directory.

Rolling prefetch can also accept a list of sequentially-related paths. That is, in the case where the full file is split up in storage due to
its file size, we can tell prefetch to treat each subset of the file as belonging to a single file.

e.g.
```
from prefetch import S3PrefetchFileSystem

fs = S3PrefetchFileSystem()

block_size = 32*2**20
prefetch_storage = [("/tmp", 5*1024)]
paths = ["large_file.pt001", "large_file.pt002", ..., "large_file.ptxxx"]
    
with fs.open(paths, block_size=block_size, prefetch_storage=prefetch_storage) as f:
  # do something with file
```

As in the case of neuroimaging, each subset of the file may contain its own header. In this case, the first element of the path list must be
a global header that applies to all the subsets, and the parameter `header_bytes`, which specifies how large the header is (in bytes) such that.
Rolling Prefetch can use this information to ensure not to read the header within each subset file.

e.g.

```
from prefetch import S3PrefetchFileSystem

fs = S3PrefetchFileSystem()

block_size = 32*2**20
prefetch_storage = [("/tmp", 5*1024)]
paths = ["global_header", "large_file.pt001", "large_file.pt002", ..., "large_file.ptxxx"]
    
with fs.open(paths, block_size=block_size, prefetch_storage=prefetch_storage, header_bytes=100*2**20) as f:
  # do something with file
```

## Installation

Clone the repository and run `pip install .` within the cloned directory.


## License

MIT


