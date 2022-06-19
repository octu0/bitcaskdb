# bitcaskdb

[![MIT License](https://img.shields.io/github/license/octu0/bitcaskdb)](https://github.com/octu0/bitcaskdb/blob/master/LICENSE)
[![GoDoc](https://godoc.org/github.com/octu0/bitcaskdb?status.svg)](https://godoc.org/github.com/octu0/bitcaskdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/octu0/bitcaskdb)](https://goreportcard.com/report/github.com/octu0/bitcaskdb)
[![Releases](https://img.shields.io/github/v/release/octu0/bitcaskdb)](https://github.com/octu0/bitcaskdb/releases)

Original code is [bitcask](https://git.mills.io/prologic/bitcask) and bitcaskdb modifies I/O operations and implement replication.  
Small Value are still operated in memory, large Value are directly I/O operation on disk.  
This makes it possible to perform Merge operations and large data store with minimal RAM utilization.

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. 
Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/).

## License

MIT, see LICENSE file for details.
