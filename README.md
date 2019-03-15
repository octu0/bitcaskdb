# bitcask

[![Build Status](https://cloud.drone.io/api/badges/prologic/bitcask/status.svg)](https://cloud.drone.io/prologic/bitcask)
[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![Sourcegraph](https://sourcegraph.com/github.com/prologic/bitcask/-/badge.svg)](https://sourcegraph.com/github.com/prologic/bitcask?badge)

A Bitcask (LSM+WAL) Key/Value Store written in Go.

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
BenchmarkGet/128B-4         	  300000	      5144 ns/op	     400 B/op	       5 allocs/op
BenchmarkGet/256B-4         	  300000	      5166 ns/op	     656 B/op	       5 allocs/op
BenchmarkGet/512B-4         	  300000	      5284 ns/op	    1200 B/op	       5 allocs/op
BenchmarkGet/1K-4           	  200000	      5779 ns/op	    2288 B/op	       5 allocs/op
BenchmarkGet/2K-4           	  200000	      6396 ns/op	    4464 B/op	       5 allocs/op
BenchmarkGet/4K-4           	  200000	      7716 ns/op	    9072 B/op	       5 allocs/op
BenchmarkGet/8K-4           	  200000	      9802 ns/op	   17776 B/op	       5 allocs/op
BenchmarkGet/16K-4          	  100000	     13299 ns/op	   34928 B/op	       5 allocs/op
BenchmarkGet/32K-4          	  100000	     21819 ns/op	   73840 B/op	       5 allocs/op

BenchmarkPut/128B-4         	  100000	     12746 ns/op	     825 B/op	       8 allocs/op
BenchmarkPut/256B-4         	  100000	     12937 ns/op	     954 B/op	       8 allocs/op
BenchmarkPut/512B-4         	  100000	     14610 ns/op	    1245 B/op	       8 allocs/op
BenchmarkPut/1K-4           	  100000	     16920 ns/op	    1825 B/op	       8 allocs/op
BenchmarkPut/2K-4           	  100000	     22075 ns/op	    2987 B/op	       8 allocs/op
BenchmarkPut/4K-4           	   30000	     40544 ns/op	    5566 B/op	       8 allocs/op
BenchmarkPut/8K-4           	   20000	     63392 ns/op	   10210 B/op	       8 allocs/op
BenchmarkPut/16K-4          	   10000	    108667 ns/op	   19244 B/op	       8 allocs/op
BenchmarkPut/32K-4          	   10000	    129256 ns/op	   41920 B/op	       8 allocs/op

BenchmarkScan-4             	 1000000	      1858 ns/op	     493 B/op	      25 allocs/op
```

For 128B values:

* ~180,000 reads/sec
*  ~80,000 writes/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## License

bitcask is licensed under the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
