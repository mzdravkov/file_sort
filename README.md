# file_sort

Command line tool for sorting big files.

## Build

Just run:

```go
go build
```



## Run

For help run:

```go
./file_sort
```



Example usage:

```go
./file_sort -memory 2048 -verbose input
```



## General description

Reads the input file in big chunks, which are then sorted in memory (using bucket sort + quicksort). The sorted chunks are written to temporary partition files, which are then merged into one big file.

### Reader

The reader reads chunks of the input file into buffers, which are sent to sorting workers. The amount of workers is fixed (by default equals to GOMAXPROC), so if there's no available sorter, the reader will wait.

### Sorters

When a chunk is sent to a sorting worker it is read line by line and each line is assigned to a bucket depending on it's first byte (so, there are 256 buckets). Each bucket is sorted internally (using quicksort from Go's standard library) and all buckets are concatenated, which yields a sorted chunk.

### Writer

There's a single writer, which receives sorted chunks from sorters and writes them to partition files.

### Merging

All partition files are read simultaneously line by line and the minimum line will be written to the output file. Buffered read and write is used to diminish the amount of reads and writes.  If the partition files are too many, the program performs a two-step merge, where an initial step merges groups of partitions into bigger intermediary files.

### Verify

If called with the flag -verify, i.e:

```go
./file_sort -verify output
```

Instead of sorting the file, it would check if the file is sorted correctly (no output if it is, error message otherwise).

## Known corner cases

The program operates under the assumption that lines in the file won't be too long (where "too long" depends on parameters). Basically, the reader has a fixed length for the buffer, which is calculated from the "assigned memory" and the number of sorters (=GOMAXPROC by default). For regular case like **-memory 2048** and **-n 8** the buffer's size would be 64MB, so it's very likely that lines would be shorter than that.