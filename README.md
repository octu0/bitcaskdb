# bitcaskdb

[![MIT License](https://img.shields.io/github/license/octu0/bitcaskdb)](https://github.com/octu0/bitcaskdb/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/octu0/bitcaskdb?status.svg)](https://godoc.org/github.com/octu0/bitcaskdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/octu0/bitcaskdb)](https://goreportcard.com/report/github.com/octu0/bitcaskdb)
[![Releases](https://img.shields.io/github/v/release/octu0/bitcaskdb)](https://github.com/octu0/bitcaskdb/releases)

Original code is [bitcask](https://git.mills.io/prologic/bitcask) and `bitcaskdb` modifies I/O operations and implement replication.  
Small Value are still operated in memory, Large Value are directly I/O operation on disk.  
This makes it possible to perform Merge operations and large data store with minimal RAM utilization.

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. 
Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/).

## Installation

```shell
go get github.com/octu0/bitcaskdb
```

## Example

`bitcaskdb` methods are implemented to use [io.Reader](https://pkg.go.dev/io#Reader) / [io.ReadCloser](https://pkg.go.dev/io#ReadCloser), etc.

```go
import (
	"bytes"
	"io"
	"fmt"

	"github.com/octu0/bitcaskdb"
)

func main() {
	db, err := bitcaskdb.Open("./data/mydb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// PutBytes() can be set using byte slice
	db.PutBytes([]byte("hello"), []byte("world"))

	// Get() returns io.ReadCloser
	r, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	defer r.Close()

	data, _ := io.ReadAll(r)

	// Put() can be specify io.Reader
	db.Put([]byte("foo"), bytes.NewReader([]byte("very large data...")))

	// PutWithTTL()/PutBytesWithTTL() can be set to data with expiration time
	db.PutWithTTL([]byte("bar"), bytes.NewReader(data), 10*time.Second)

	// Sync() flushes all buffers to disk
	db.Sync()

	r, err := db.Get([]byte("foo"))
	if err != nil {
		panic(err)
	}
	defer r.Close()

	head := make([]byte, 4)
	r.Read(head)

	// Delete() can delete data with key
	db.Delete([]byte("foo"))

	// RunGC() deletes all expired keys
	db.RunGC()

	// Merge() rebuilds databases and reclaims disk space
	db.Merge()
}
```

## Benchmark

`bitcaskdb` is tuned for larger sizes of Value, in particular there is a major improvement for inputs and outputs using [io.Reader](https://pkg.go.dev/io#Reader).  
Default Buffer size is aligned to 128KB, this value can be changed with [runtime.Context](https://pkg.go.dev/github.com/octu0/bitcaskdb#WithRuntimeContext).

```
goos: darwin
goarch: amd64
pkg: github.com/octu0/bitcaskdb
cpu: Intel(R) Core(TM) i7-8569U CPU @ 2.80GHz

BenchmarkGet
BenchmarkGet/prologic/bitcask/128B
BenchmarkGet/prologic/bitcask/128B-8      1317308       897.5 ns/op   142.62 MB/s       160 B/op     1 allocs/op
BenchmarkGet/prologic/bitcask/256B
BenchmarkGet/prologic/bitcask/256B-8      1229084       973.1 ns/op   263.08 MB/s       288 B/op     1 allocs/op
BenchmarkGet/prologic/bitcask/128K
BenchmarkGet/prologic/bitcask/128K-8        34060       30690 ns/op  4270.86 MB/s    139264 B/op     1 allocs/op
BenchmarkGet/prologic/bitcask/256K
BenchmarkGet/prologic/bitcask/256K-8        18928       56895 ns/op  4607.53 MB/s    270337 B/op     1 allocs/op
BenchmarkGet/prologic/bitcask/512K
BenchmarkGet/prologic/bitcask/512K-8         9092      130977 ns/op  4002.91 MB/s    532483 B/op     1 allocs/op
BenchmarkGet/octu0/bitcaskdb/128B
BenchmarkGet/octu0/bitcaskdb/128B-8        102139       54958 ns/op     2.33 MB/s    205145 B/op    21 allocs/op
BenchmarkGet/octu0/bitcaskdb/256B
BenchmarkGet/octu0/bitcaskdb/256B-8         35295       45820 ns/op     5.59 MB/s    206286 B/op    21 allocs/op
BenchmarkGet/octu0/bitcaskdb/128K
BenchmarkGet/octu0/bitcaskdb/128K-8         32757       40773 ns/op  3214.66 MB/s    199695 B/op    21 allocs/op
BenchmarkGet/octu0/bitcaskdb/256K
BenchmarkGet/octu0/bitcaskdb/256K-8         25388       49611 ns/op  5284.03 MB/s    214647 B/op    21 allocs/op
BenchmarkGet/octu0/bitcaskdb/512K
BenchmarkGet/octu0/bitcaskdb/512K-8         18439       69490 ns/op  7544.78 MB/s    215778 B/op    21 allocs/op

BenchmarkPut
BenchmarkPut/prologic/bitcask/WithNosync/128B
BenchmarkPut/prologic/bitcask/WithNosync/128B-8       85623       13452 ns/op     9.52 MB/s     41 B/op     2 allocs/op
BenchmarkPut/prologic/bitcask/WithNosync/256B
BenchmarkPut/prologic/bitcask/WithNosync/256B-8       69417       21407 ns/op    11.96 MB/s     43 B/op     2 allocs/op
BenchmarkPut/prologic/bitcask/WithNosync/128K
BenchmarkPut/prologic/bitcask/WithNosync/128K-8         140     8566745 ns/op    15.30 MB/s   1569 B/op     9 allocs/op
BenchmarkPut/prologic/bitcask/WithNosync/256K
BenchmarkPut/prologic/bitcask/WithNosync/256K-8         100    16770433 ns/op    15.63 MB/s   3052 B/op    16 allocs/op
BenchmarkPut/prologic/bitcask/WithNosync/512K
BenchmarkPut/prologic/bitcask/WithNosync/512K-8         100    34380284 ns/op    15.25 MB/s   6193 B/op    31 allocs/op
BenchmarkPut/octu0/bitcaskdb/WithNosync/128B
BenchmarkPut/octu0/bitcaskdb/WithNosync/128B-8       333992        3548 ns/op    36.07 MB/s    200 B/op     9 allocs/op
BenchmarkPut/octu0/bitcaskdb/WithNosync/256B
BenchmarkPut/octu0/bitcaskdb/WithNosync/256B-8       305065        3900 ns/op    65.64 MB/s    208 B/op    10 allocs/op
BenchmarkPut/octu0/bitcaskdb/WithNosync/128K
BenchmarkPut/octu0/bitcaskdb/WithNosync/128K-8        10000      242319 ns/op   540.91 MB/s    594 B/op    10 allocs/op
BenchmarkPut/octu0/bitcaskdb/WithNosync/256K
BenchmarkPut/octu0/bitcaskdb/WithNosync/256K-8         7059      447583 ns/op   585.69 MB/s    461 B/op    10 allocs/op
BenchmarkPut/octu0/bitcaskdb/WithNosync/512K
BenchmarkPut/octu0/bitcaskdb/WithNosync/512K-8         3835      812899 ns/op   644.96 MB/s    848 B/op    10 allocs/op
```

## License

MIT, see LICENSE file for details.
