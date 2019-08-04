// Package bitcask implements a high-performance key-value store based on a
// WAL and LSM.
//
// By default, the client assumes a default configuration regarding maximum key size,
// maximum value size, maximum datafile size, and memory pools to avoid allocations.
// Refer to Constants section to know default values.
//
// For extra performance, configure the memory pool option properly. This option
// requires to specify the maximum number of concurrent use of the package. Failing to
// set a high-enough value would impact latency and throughput. Likewise, overestimating
// would yield in an unnecessary big memory footprint.
// The default configuration doesn't use a memory pool.
package bitcask
