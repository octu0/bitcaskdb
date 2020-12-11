# bitcask

![](https://github.com/prologic/bitcask/workflows/Coverage/badge.svg)
![](https://github.com/prologic/bitcask/workflows/Docker/badge.svg)
![](https://github.com/prologic/bitcask/workflows/Go/badge.svg)
![](https://github.com/prologic/bitcask/workflows/ReviewDog/badge.svg)

[![CodeCov](https://codecov.io/gh/prologic/bitcask/branch/master/graph/badge.svg)](https://codecov.io/gh/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/prologic/bitcask)](https://goreportcard.com/report/prologic/bitcask)
[![codebeat badge](https://codebeat.co/badges/15fba8a5-3044-4f40-936f-9e0f5d5d1fd9)](https://codebeat.co/projects/github-com-prologic-bitcask-master)
[![GoDoc](https://godoc.org/github.com/prologic/bitcask?status.svg)](https://godoc.org/github.com/prologic/bitcask) 
[![GitHub license](https://img.shields.io/github/license/prologic/bitcask.svg)](https://github.com/prologic/bitcask)
[![Sourcegraph](https://sourcegraph.com/github.com/prologic/bitcask/-/badge.svg)](https://sourcegraph.com/github.com/prologic/bitcask?badge)
[![TODOs](https://img.shields.io/endpoint?url=https://api.tickgit.com/badge?repo=github.com/prologic/bitcask)](https://www.tickgit.com/browse?repo=github.com/prologic/bitcask)

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

```sh
$ git clone https://github.com/prologic/bitcask.git
$ make
```

## Install

```sh
$ go get github.com/prologic/bitcask
```

## Usage (library)

Install the package into your project:

```sh
$ go get github.com/prologic/bitcask
```

```go
package main

import (
	"log"
	"github.com/prologic/bitcask"
)

func main() {
    db, _ := bitcask.Open("/tmp/db")
    defer db.Close()
    db.Put([]byte("Hello"), []byte("World"))
    val, _ := db.Get([]byte("Hello"))
    log.Printf(string(val))
}
```

See the [godoc](https://godoc.org/github.com/prologic/bitcask) for further
documentation and other examples.

## Usage (tool)

```sh
$ bitcask -p /tmp/db set Hello World
$ bitcask -p /tmp/db get Hello
World
```

## Usage (server)

There is also a builtin very  simple Redis-compatible server called `bitcaskd`:

```sh
$ ./bitcaskd ./tmp
INFO[0000] starting bitcaskd v0.0.7@146f777              bind=":6379" path=./tmp
```

Example session:

```sh
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

```sh
$ docker pull prologic/bitcask
$ docker run -d -p 6379:6379 prologic/bitcask
```

## Performance

Benchmarks run on a 11" Macbook with a 1.4Ghz Intel Core i7:

```sh
$ make bench
...
goos: darwin
goarch: amd64
pkg: github.com/prologic/bitcask

BenchmarkGet/128B-4         	  316515	      3263 ns/op	  39.22 MB/s	     160 B/op	       1 allocs/op
BenchmarkGet/256B-4         	  382551	      3204 ns/op	  79.90 MB/s	     288 B/op	       1 allocs/op
BenchmarkGet/512B-4         	  357216	      3835 ns/op	 133.51 MB/s	     576 B/op	       1 allocs/op
BenchmarkGet/1K-4           	  274958	      4429 ns/op	 231.20 MB/s	    1152 B/op	       1 allocs/op
BenchmarkGet/2K-4           	  227764	      5013 ns/op	 408.55 MB/s	    2304 B/op	       1 allocs/op
BenchmarkGet/4K-4           	  187557	      5534 ns/op	 740.15 MB/s	    4864 B/op	       1 allocs/op
BenchmarkGet/8K-4           	  153546	      7652 ns/op	1070.56 MB/s	    9472 B/op	       1 allocs/op
BenchmarkGet/16K-4          	  115549	     10272 ns/op	1594.95 MB/s	   18432 B/op	       1 allocs/op
BenchmarkGet/32K-4          	   69592	     16405 ns/op	1997.39 MB/s	   40960 B/op	       1 allocs/op

BenchmarkPut/128BNoSync-4   	  123519	     11094 ns/op	  11.54 MB/s	      49 B/op	       2 allocs/op
BenchmarkPut/256BNoSync-4   	   84662	     13398 ns/op	  19.11 MB/s	      50 B/op	       2 allocs/op
BenchmarkPut/1KNoSync-4     	   46345	     24855 ns/op	  41.20 MB/s	      58 B/op	       2 allocs/op
BenchmarkPut/2KNoSync-4     	   28820	     43817 ns/op	  46.74 MB/s	      68 B/op	       2 allocs/op
BenchmarkPut/4KNoSync-4     	   13976	     90059 ns/op	  45.48 MB/s	      89 B/op	       2 allocs/op
BenchmarkPut/8KNoSync-4     	    7852	    155101 ns/op	  52.82 MB/s	     130 B/op	       2 allocs/op
BenchmarkPut/16KNoSync-4    	    4848	    238113 ns/op	  68.81 MB/s	     226 B/op	       2 allocs/op
BenchmarkPut/32KNoSync-4    	    2564	    391483 ns/op	  83.70 MB/s	     377 B/op	       3 allocs/op

BenchmarkPut/128BSync-4     	     260	   4611273 ns/op	   0.03 MB/s	      48 B/op	       2 allocs/op
BenchmarkPut/256BSync-4     	     265	   4665506 ns/op	   0.05 MB/s	      48 B/op	       2 allocs/op
BenchmarkPut/1KSync-4       	     256	   4757334 ns/op	   0.22 MB/s	      48 B/op	       2 allocs/op
BenchmarkPut/2KSync-4       	     255	   4996788 ns/op	   0.41 MB/s	      92 B/op	       2 allocs/op
BenchmarkPut/4KSync-4       	     222	   5136481 ns/op	   0.80 MB/s	      98 B/op	       2 allocs/op
BenchmarkPut/8KSync-4       	     223	   5530824 ns/op	   1.48 MB/s	      99 B/op	       2 allocs/op
BenchmarkPut/16KSync-4      	     213	   5717880 ns/op	   2.87 MB/s	     202 B/op	       2 allocs/op
BenchmarkPut/32KSync-4      	     211	   5835948 ns/op	   5.61 MB/s	     355 B/op	       3 allocs/op

BenchmarkScan-4             	  568696	      2036 ns/op	     392 B/op	      33 allocs/op
PASS
```

For 128B values:

* ~300,000 reads/sec
* ~90,000 writes/sec
* ~490,000 scans/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## Support

Support the ongoing development of Bitcask!

**Sponser**

- Become a [Sponsor](https://www.patreon.com/prologic)

## Stargazers over time

[![Stargazers over time](https://starcharts.herokuapp.com/prologic/bitcask.svg)](https://starcharts.herokuapp.com/prologic/bitcask)

## Contributors

Thank you to all those that have contributed to this project, battle-tested it, used it in their own projects or products, fixed bugs, improved performance and even fix tiny typos in documentation! Thank you and keep contributing!

You can find an [AUTHORS](/AUTHORS) file where we keep a list of contributors to the project. If you contriibute a PR please consider adding your name there. There is also Github's own [Contributors](https://github.com/prologic/bitcask/graphs/contributors) statistics.

[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/0)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/0)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/1)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/1)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/2)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/2)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/3)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/3)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/4)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/4)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/5)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/5)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/6)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/6)
[![](https://sourcerer.io/fame/prologic/prologic/bitcask/images/7)](https://sourcerer.io/fame/prologic/prologic/bitcask/links/7)

## Related Projects

- [bitraft](https://github.com/prologic/bitraft) -- A Distributed Key/Value store (_using Raft_) with a Redis compatible protocol.
- [bitcaskfs](https://github.com/prologic/bitcaskfs) -- A FUSE filesystem for mounting a Bitcask database.
- [bitcask-bench](https://github.com/prologic/bitcask-bench) -- A benchmarking tool comparing Bitcask and several other Go key/value libraries.

## License

bitcask is licensed under the term of the [MIT License](https://github.com/prologic/bitcask/blob/master/LICENSE)
