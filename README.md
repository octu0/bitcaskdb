# bitcask

[![Build Status](https://cloud.drone.io/api/badges/prologic/bitcask/status.svg)](https://cloud.drone.io/prologic/bitcask)
[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![GitHub license](https://img.shields.io/github/license/prologic/bitcask.svg)](https://github.com/prologic/bitcask)

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/)

For a more feature-complete Redis-compatible server, distributed key/value store have a look at [Bitraft](https://github.com/prologic/bitraft) which uses this library as its backend. Use [Bitcask](https://github.com/prologic/bitcask) as a starting point or if you want to embed in your application, use [Bitraft](https://github.com/prologic/bitraft) if you need a complete server/client solution with high availability with a Redis-compatible API.

## Features

* Embeddable (`import "github.com/prologic/bitcask"`)
* Builtin CLI (`bitcask`)
* Builtin Redis-compatible server (`bitcaskd`)
* Predictable read/write performance
* Low latency
* High throughput (See: [Performance](README.md#Performance) )

## Development

1. Get the source

```#!sh
$ git clone https://github.com/prologic/bitcask.git
```

2. Install required tools

This library uses [Protobuf](https://github.com/protocolbuffers/protobuf) to serialize data on disk. Please follow the
instructions for installing `protobuf` on your system. You will also need the
following Go libraries/tools to generate Go code from Protobuf defs:
- [protoc-gen-go](https://github.com/golang/protobuf)

3. Build the project

```#!sh
$ make
```

This will invoke `go generate` and `go build`.

## Install

```#!sh
$ go get github.com/prologic/bitcask
```

## Usage (library)

Install the package into your project:

```#!sh
$ go get github.com/prologic/bitcask
```

```#!go
package main

import "github.com/prologic/bitcask"

func main() {
    db, _ := bitcask.Open("/tmp/db")
    defer db.Close()
    db.Put("Hello", []byte("World"))
    val, _ := db.Get("Hello")
}
```

See the [godoc](https://godoc.org/github.com/prologic/bitcask) for further
documentation and other examples.

## Usage (tool)

```#!sh
$ bitcask -p /tmp/db set Hello World
$ bitcask -p /tmp/db get Hello
World
```

## Usage (server)

There is also a builtin very  simple Redis-compatible server called `bitcaskd`:

```#!sh
$ ./bitcaskd ./tmp
INFO[0000] starting bitcaskd v0.0.7@146f777              bind=":6379" path=./tmp
```

Example session:

```#!sh
$ telnet localhost 6379
Trying ::1...
Connected to localhost.
Escape character is '^]'.
SET foo bar
+OK
GET foo
$3
bar
DEL foo
:1
GET foo
$-1
PING
+PONG
QUIT
+OK
Connection closed by foreign host.
```

## Docker

You can also use the [Bitcask Docker Image](https://cloud.docker.com/u/prologic/repository/docker/prologic/bitcask):

```#!sh
$ docker pull prologic/bitcask
$ docker run -d -p 6379:6379 prologic/bitcask
```

## Performance

Benchmarks run on a 11" Macbook with a 1.4Ghz Intel Core i7:

```#!sh
$ make bench
...
goos: darwin
goarch: amd64
pkg: github.com/prologic/bitcask

BenchmarkGet/128B-4         	  300000	      3913 ns/op	  32.71 MB/s	     387 B/op	       4 allocs/op
BenchmarkGet/128BWithPool-4 	  300000	      4143 ns/op	  30.89 MB/s	     227 B/op	       3 allocs/op
BenchmarkGet/256B-4         	  300000	      3919 ns/op	  65.31 MB/s	     643 B/op	       4 allocs/op
BenchmarkGet/256BWithPool-4 	  300000	      4270 ns/op	  59.95 MB/s	     355 B/op	       3 allocs/op
BenchmarkGet/512B-4         	  300000	      4248 ns/op	 120.52 MB/s	    1187 B/op	       4 allocs/op
BenchmarkGet/512BWithPool-4 	  300000	      4676 ns/op	 109.48 MB/s	     611 B/op	       3 allocs/op
BenchmarkGet/1K-4           	  200000	      5248 ns/op	 195.10 MB/s	    2275 B/op	       4 allocs/op
BenchmarkGet/1KWithPool-4   	  200000	      5270 ns/op	 194.28 MB/s	    1123 B/op	       3 allocs/op
BenchmarkGet/2K-4           	  200000	      6229 ns/op	 328.74 MB/s	    4451 B/op	       4 allocs/op
BenchmarkGet/2KWithPool-4   	  200000	      6282 ns/op	 325.99 MB/s	    2147 B/op	       3 allocs/op
BenchmarkGet/4K-4           	  200000	      9027 ns/op	 453.74 MB/s	    9059 B/op	       4 allocs/op
BenchmarkGet/4KWithPool-4   	  200000	      8906 ns/op	 459.87 MB/s	    4195 B/op	       3 allocs/op
BenchmarkGet/8K-4           	  100000	     12024 ns/op	 681.28 MB/s	   17763 B/op	       4 allocs/op
BenchmarkGet/8KWithPool-4   	  200000	     11103 ns/op	 737.79 MB/s	    8291 B/op	       3 allocs/op
BenchmarkGet/16K-4          	  100000	     16844 ns/op	 972.65 MB/s	   34915 B/op	       4 allocs/op
BenchmarkGet/16KWithPool-4  	  100000	     14575 ns/op	1124.10 MB/s	   16483 B/op	       3 allocs/op
BenchmarkGet/32K-4          	   50000	     27770 ns/op	1179.97 MB/s	   73827 B/op	       4 allocs/op
BenchmarkGet/32KWithPool-4  	  100000	     24495 ns/op	1337.74 MB/s	   32867 B/op	       3 allocs/op

BenchmarkPut/128B-4         	  100000	     17492 ns/op	   7.32 MB/s	     441 B/op	       6 allocs/op
BenchmarkPut/256B-4         	  100000	     17234 ns/op	  14.85 MB/s	     571 B/op	       6 allocs/op
BenchmarkPut/512B-4         	  100000	     22837 ns/op	  22.42 MB/s	     861 B/op	       6 allocs/op
BenchmarkPut/1K-4           	   50000	     30333 ns/op	  33.76 MB/s	    1443 B/op	       6 allocs/op
BenchmarkPut/2K-4           	   30000	     45304 ns/op	  45.21 MB/s	    2606 B/op	       6 allocs/op
BenchmarkPut/4K-4           	   20000	     83953 ns/op	  48.79 MB/s	    5187 B/op	       6 allocs/op
BenchmarkPut/8K-4           	   10000	    142142 ns/op	  57.63 MB/s	    9845 B/op	       6 allocs/op
BenchmarkPut/16K-4          	    5000	    206722 ns/op	  79.26 MB/s	   18884 B/op	       6 allocs/op
BenchmarkPut/32K-4          	    5000	    361108 ns/op	  90.74 MB/s	   41582 B/op	       7 allocs/op

BenchmarkScan-4             	 1000000	      1679 ns/op	     408 B/op	      16 allocs/op
PASS
```

For 128B values:

* ~200,000 reads/sec
* ~50,000 writes/sec

The full benchmark above shows linear performance as you increase key/value sizes. Memory pooling starts to become advantageous for larger values.

## Contributors

Thank you to all those that have contributed to this project, battle-tested it, used it in their own projects or pdocuts, fixed bugs, improved performance and even fix tiny tpyos in documentation! Thank you and keep contirbuting!

You can find an [AUTHORS](/AUTHORS) file where we keep a list of contributors to the project. If you contriibute a PR please consider adding your name there. There is also Github's own [Contributors](https://github.com/prologic/bitcask/graphs/contributors) statistics.

## License

bitcask is licensed under the term of the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
