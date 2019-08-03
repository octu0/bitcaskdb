# bitcask

[![Build Status](https://cloud.drone.io/api/badges/prologic/bitcask/status.svg)](https://cloud.drone.io/prologic/bitcask)
[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![GitHub license](https://img.shields.io/github/license/prologic/bitcask.svg)](https://github.com/prologic/bitcask)

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/). 🗃️

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

```#!bash
$ git clone https://github.com/prologic/bitcask.git
```

2. Install required tools

This library uses [Protobuf](https://github.com/protocolbuffers/protobuf) to serialize data on disk. Please follow the
instructions for installing `protobuf` on your system. You will also need the
following Go libraries/tools to generate Go code from Protobuf defs:
- [protoc-gen-go](https://github.com/golang/protobuf)

3. Build the project

```#!bash
$ make
```

This will invoke `go generate` and `go build`.

## Install

```#!bash
$ go get github.com/prologic/bitcask
```

## Usage (library)

Install the package into your project:

```#!bash
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

```#!bash
$ bitcask -p /tmp/db set Hello World
$ bitcask -p /tmp/db get Hello
World
```

## Usage (server)

There is also a builtin very  simple Redis-compatible server called `bitcaskd`:

```#!bash
$ ./bitcaskd ./tmp
INFO[0000] starting bitcaskd v0.0.7@146f777              bind=":6379" path=./tmp
```

Example session:

```
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

```#!bash
$ docker pull prologic/bitcask
$ docker run -d -p 6379:6379 prologic/bitcask
```

## Performance

Benchmarks run on a 11" Macbook with a 1.4Ghz Intel Core i7:

```
$ make bench
...
BenchmarkGet/128B-4         	  300000	      4071 ns/op	  31.43 MB/s	     608 B/op	       7 allocs/op
BenchmarkGet/256B-4         	  300000	      4700 ns/op	  54.46 MB/s	     992 B/op	       7 allocs/op
BenchmarkGet/512B-4         	  300000	      4915 ns/op	 104.17 MB/s	    1824 B/op	       7 allocs/op
BenchmarkGet/1K-4           	  200000	      5064 ns/op	 202.20 MB/s	    3488 B/op	       7 allocs/op
BenchmarkGet/2K-4           	  200000	      6276 ns/op	 326.31 MB/s	    6816 B/op	       7 allocs/op
BenchmarkGet/4K-4           	  200000	      8960 ns/op	 457.11 MB/s	   13984 B/op	       7 allocs/op
BenchmarkGet/8K-4           	  100000	     12465 ns/op	 657.16 MB/s	   27296 B/op	       7 allocs/op
BenchmarkGet/16K-4          	  100000	     19233 ns/op	 851.84 MB/s	   53408 B/op	       7 allocs/op
BenchmarkGet/32K-4          	   50000	     33106 ns/op	 989.77 MB/s	  114848 B/op	       7 allocs/op

BenchmarkPut/128B-4         	  100000	     13659 ns/op	   9.37 MB/s	     409 B/op	       6 allocs/op
BenchmarkPut/256B-4         	  100000	     14854 ns/op	  17.23 MB/s	     539 B/op	       6 allocs/op
BenchmarkPut/512B-4         	  100000	     20823 ns/op	  24.59 MB/s	     829 B/op	       6 allocs/op
BenchmarkPut/1K-4           	   50000	     28086 ns/op	  36.46 MB/s	    1411 B/op	       6 allocs/op
BenchmarkPut/2K-4           	   30000	     40797 ns/op	  50.20 MB/s	    2574 B/op	       6 allocs/op
BenchmarkPut/4K-4           	   20000	     75518 ns/op	  54.24 MB/s	    5155 B/op	       6 allocs/op
BenchmarkPut/8K-4           	   10000	    122544 ns/op	  66.85 MB/s	    9811 B/op	       6 allocs/op
BenchmarkPut/16K-4          	   10000	    201167 ns/op	  81.44 MB/s	   18851 B/op	       6 allocs/op
BenchmarkPut/32K-4          	    5000	    350850 ns/op	  93.40 MB/s	   41565 B/op	       7 allocs/op

BenchmarkScan-4             	 1000000	      1867 ns/op	     493 B/op	      25 allocs/op
```

For 128B values:

* ~400,000 reads/sec
* ~130,000 writes/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## License

bitcask is licensed under the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
