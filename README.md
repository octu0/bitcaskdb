# bitcask

[![Build Status](https://cloud.drone.io/api/badges/prologic/bitcask/status.svg)](https://cloud.drone.io/prologic/bitcask)
[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![Sourcegraph](https://sourcegraph.com/github.com/prologic/bitcask/-/badge.svg)](https://sourcegraph.com/github.com/prologic/bitcask?badge)

A high performance Key/Value store written in Go with a predictable read/write performance and high throughput. Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/).

## Features

* Embeddable
* Builtin CLI
* Builtin Redis-compatible server
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

import (
    "log"

    "github.com/prologic/bitcask"
)

func main() {
    db, _ := bitcask.Open("/tmp/db")
    db.Set("Hello", []byte("World"))
    db.Close()
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
BenchmarkGet/128B-4         	  300000	      5178 ns/op	     400 B/op	       5 allocs/op
BenchmarkGet/256B-4         	  300000	      5273 ns/op	     656 B/op	       5 allocs/op
BenchmarkGet/512B-4         	  200000	      5368 ns/op	    1200 B/op	       5 allocs/op
BenchmarkGet/1K-4           	  200000	      5800 ns/op	    2288 B/op	       5 allocs/op
BenchmarkGet/2K-4           	  200000	      6766 ns/op	    4464 B/op	       5 allocs/op
BenchmarkGet/4K-4           	  200000	      7857 ns/op	    9072 B/op	       5 allocs/op
BenchmarkGet/8K-4           	  200000	      9538 ns/op	   17776 B/op	       5 allocs/op
BenchmarkGet/16K-4          	  100000	     13188 ns/op	   34928 B/op	       5 allocs/op
BenchmarkGet/32K-4          	  100000	     21620 ns/op	   73840 B/op	       5 allocs/op

BenchmarkPut/128B-4         	  200000	      7875 ns/op	     409 B/op	       6 allocs/op
BenchmarkPut/256B-4         	  200000	      8712 ns/op	     538 B/op	       6 allocs/op
BenchmarkPut/512B-4         	  200000	      9832 ns/op	     829 B/op	       6 allocs/op
BenchmarkPut/1K-4           	  100000	     13105 ns/op	    1410 B/op	       6 allocs/op
BenchmarkPut/2K-4           	  100000	     18601 ns/op	    2572 B/op	       6 allocs/op
BenchmarkPut/4K-4           	   50000	     36631 ns/op	    5151 B/op	       6 allocs/op
BenchmarkPut/8K-4           	   30000	     56128 ns/op	    9798 B/op	       6 allocs/op
BenchmarkPut/16K-4          	   20000	     83209 ns/op	   18834 B/op	       6 allocs/op
BenchmarkPut/32K-4          	   10000	    135899 ns/op	   41517 B/op	       6 allocs/op

BenchmarkScan-4             	 1000000	      1851 ns/op	     493 B/op	      25 allocs/op
```

For 128B values:

* ~200,000 reads/sec
* ~130,000 writes/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## License

bitcask is licensed under the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
