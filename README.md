# bitcask

[![Build Status](https://cloud.drone.io/api/badges/prologic/bitcask/status.svg)](https://cloud.drone.io/prologic/bitcask)
[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![Sourcegraph](https://sourcegraph.com/github.com/prologic/bitcask/-/badge.svg)](https://sourcegraph.com/github.com/prologic/bitcask?badge)

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/).

## Features

* Embeddable (`import "github.com/prologic/bitcask"`)
* Builtin CLI (`bitcask`)
* Builtin Redis-compatible server (`bitcaskd`)
* Predictable read/write performance
* Low latecny
* High throughput (See: [Performance](README.md#Performance)

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
    db.Set("Hello", []byte("World"))
    val, _ := db.Get("hello")
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

## Performance

Benchmarks run on a 11" Macbook with a 1.4Ghz Intel Core i7:

```
$ make bench
...
BenchmarkGet/128B-4         	  300000	      3737 ns/op	    1632 B/op	      16 allocs/op
BenchmarkGet/256B-4         	  300000	      4183 ns/op	    2016 B/op	      16 allocs/op
BenchmarkGet/512B-4         	  300000	      4295 ns/op	    2848 B/op	      16 allocs/op
BenchmarkGet/1K-4           	  300000	      4455 ns/op	    4512 B/op	      16 allocs/op
BenchmarkGet/2K-4           	  300000	      5536 ns/op	    7841 B/op	      16 allocs/op
BenchmarkGet/4K-4           	  200000	      7101 ns/op	   15010 B/op	      16 allocs/op
BenchmarkGet/8K-4           	  200000	     10664 ns/op	   28325 B/op	      16 allocs/op
BenchmarkGet/16K-4          	  100000	     18173 ns/op	   54442 B/op	      16 allocs/op
BenchmarkGet/32K-4          	   50000	     33081 ns/op	  115893 B/op	      16 allocs/op

BenchmarkPut/128B-4         	  200000	      7967 ns/op	     409 B/op	       6 allocs/op
BenchmarkPut/256B-4         	  200000	      8563 ns/op	     538 B/op	       6 allocs/op
BenchmarkPut/512B-4         	  200000	      9678 ns/op	     829 B/op	       6 allocs/op
BenchmarkPut/1K-4           	  200000	     12786 ns/op	    1410 B/op	       6 allocs/op
BenchmarkPut/2K-4           	  100000	     18582 ns/op	    2572 B/op	       6 allocs/op
BenchmarkPut/4K-4           	   50000	     35335 ns/op	    5151 B/op	       6 allocs/op
BenchmarkPut/8K-4           	   30000	     56047 ns/op	    9797 B/op	       6 allocs/op
BenchmarkPut/16K-4          	   20000	     86137 ns/op	   18834 B/op	       6 allocs/op
BenchmarkPut/32K-4          	   10000	    140162 ns/op	   41517 B/op	       6 allocs/op

BenchmarkScan-4             	 1000000	      1885 ns/op	     493 B/op	      25 allocs/op
```

For 128B values:

* ~270,000 reads/sec
* ~130,000 writes/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## License

bitcask is licensed under the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
