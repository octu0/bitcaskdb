package bitcaskdb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"git.mills.io/prologic/bitcask"
)

type benchmarkTestCase struct {
	name string
	size int
}

type (
	factoryBenchMiddlewareGet func() benchMiddlewareGet
	benchMiddlewareGet        interface {
		Name() string
		Setup(testdir string, key []byte, value []byte) error
		Get(key []byte, expectValue []byte) (bool, error)
		Teardown()
	}

	factoryBenchMiddlewarePut func() benchMiddlewarePut
	benchMiddlewarePut        interface {
		Name() string
		Setup(testdir string, withSync bool) error
		Put(key []byte, value []byte) error
		Teardown()
	}

	factoryBenchMiddlewarePutFromFile func() benchMiddlewarePutFromFile
	benchMiddlewarePutFromFile        interface {
		Name() string
		Setup(testdir string, withSync bool) error
		Put(key []byte, file *os.File) error
		Teardown()
	}

	factoryBenchMiddlewareMerge func() benchMiddlewareMerge
	benchMiddlewareMerge        interface {
		Name() string
		Setup(testdir string) error
		LoadData(value []byte, putCount int) error
		Merge() error
		Teardown()
	}

	factoryBenchMiddlewareMergeThroughput func() benchMiddlewareMergeThroughput
	benchMiddlewareMergeThroughput        interface {
		Name() string
		Setup(testdir string, datafilesize int) error
		Put(key, value []byte) time.Duration
		Get(key []byte) time.Duration
		Merge() error
		Teardown()
	}
)

// ----- {{{ octu0/bitcaskdb

type benchOctu0bitcaskdbGet struct {
	db  *Bitcask
	buf *bytes.Buffer
}

func (t *benchOctu0bitcaskdbGet) Name() string {
	return "octu0/bitcaskdb"
}

func (t *benchOctu0bitcaskdbGet) Setup(testdir string, key []byte, value []byte) error {
	db, err := Open(testdir)
	if err != nil {
		return err
	}
	if err := db.PutBytes(key, value); err != nil {
		return err
	}
	t.db = db
	t.buf = bytes.NewBuffer(nil)
	return nil
}

func (t *benchOctu0bitcaskdbGet) Teardown() {
	t.db.Close()
	t.buf.Reset()
}

func (t *benchOctu0bitcaskdbGet) Get(key []byte, expectValue []byte) (bool, error) {
	r, err := t.db.Get(key)
	if err != nil {
		return false, err
	}
	t.buf.Reset()
	t.buf.ReadFrom(r)

	return bytes.Equal(expectValue, t.buf.Bytes()), nil
}

type benchOctu0bitcaskdbPut struct {
	db *Bitcask
}

func (t *benchOctu0bitcaskdbPut) Name() string {
	return "octu0/bitcaskdb"
}

func (t *benchOctu0bitcaskdbPut) Setup(testdir string, enable bool) error {
	db, err := Open(testdir, WithSync(enable))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchOctu0bitcaskdbPut) Teardown() {
	t.db.Close()
}

func (t *benchOctu0bitcaskdbPut) Put(key []byte, value []byte) error {
	return t.db.PutBytes(key, value)
}

type benchOctu0bitcaskdbPutFromFile struct {
	db *Bitcask
}

func (t *benchOctu0bitcaskdbPutFromFile) Name() string {
	return "octu0/bitcaskdb"
}

func (t *benchOctu0bitcaskdbPutFromFile) Setup(testdir string, enable bool) error {
	db, err := Open(testdir, WithSync(enable))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchOctu0bitcaskdbPutFromFile) Teardown() {
	t.db.Close()
}

func (t *benchOctu0bitcaskdbPutFromFile) Put(key []byte, file *os.File) error {
	return t.db.Put(key, file)
}

type benchOctu0bitcaskdbMerge struct {
	db *Bitcask
}

func (t *benchOctu0bitcaskdbMerge) Name() string {
	return "octu0/bitcaskdb"
}

func (t *benchOctu0bitcaskdbMerge) Setup(testdir string) error {
	db, err := Open(testdir)
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchOctu0bitcaskdbMerge) Teardown() {
	t.db.Close()
}

func (t *benchOctu0bitcaskdbMerge) LoadData(value []byte, putCount int) error {
	b := bytes.NewReader(value)
	for i := 0; i < putCount; i += 1 {
		if err := t.db.Put([]byte(fmt.Sprintf("%d", i)), b); err != nil {
			return err
		}
		b.Seek(0, io.SeekStart)
	}
	return nil
}

func (t *benchOctu0bitcaskdbMerge) Merge() error {
	return t.db.Merge()
}

type benchOctu0bitcaskdbMergeThroughput struct {
	db *Bitcask
}

func (t *benchOctu0bitcaskdbMergeThroughput) Name() string {
	return "octu0/bitcaskdb"
}

func (t *benchOctu0bitcaskdbMergeThroughput) Setup(testdir string, datafilesize int) error {
	db, err := Open(testdir, WithMaxDatafileSize(datafilesize))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchOctu0bitcaskdbMergeThroughput) Teardown() {
	t.db.Close()
}

func (t *benchOctu0bitcaskdbMergeThroughput) Put(key, value []byte) time.Duration {
	start := time.Now()
	_ = t.db.PutBytes(key, value)
	return time.Since(start)
}

func (t *benchOctu0bitcaskdbMergeThroughput) Get(key []byte) time.Duration {
	start := time.Now()
	v, err := t.db.Get(key)
	if err == nil {
		defer v.Close()
	}

	return time.Since(start)
}

func (t *benchOctu0bitcaskdbMergeThroughput) Merge() error {
	return t.db.Merge()
}

// ----- }}} octu0/bitcaskdb

// ----- {{{ prologic/bitcask

type benchPrologicBitcaskGet struct {
	db  *bitcask.Bitcask
	buf *bytes.Buffer
}

func (t *benchPrologicBitcaskGet) Name() string {
	return "prologic/bitcask"
}

func (t *benchPrologicBitcaskGet) Setup(testdir string, key []byte, value []byte) error {
	db, err := bitcask.Open(testdir, bitcask.WithMaxValueSize(512*1024))
	if err != nil {
		return err
	}
	if err := db.Put(key, value); err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchPrologicBitcaskGet) Teardown() {
	t.db.Close()
}

func (t *benchPrologicBitcaskGet) Get(key []byte, expectValue []byte) (bool, error) {
	value, err := t.db.Get(key)
	if err != nil {
		return false, err
	}

	return bytes.Equal(expectValue, value), nil
}

type benchPrologicBitcaskPut struct {
	db *bitcask.Bitcask
}

func (t *benchPrologicBitcaskPut) Name() string {
	return "prologic/bitcask"
}

func (t *benchPrologicBitcaskPut) Setup(testdir string, enable bool) error {
	db, err := bitcask.Open(testdir, bitcask.WithSync(enable), bitcask.WithMaxValueSize(512*1024))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchPrologicBitcaskPut) Teardown() {
	t.db.Close()
}

func (t *benchPrologicBitcaskPut) Put(key []byte, value []byte) error {
	return t.db.Put(key, value)
}

type benchPrologicBitcaskPutFromFile struct {
	db *bitcask.Bitcask
}

func (t *benchPrologicBitcaskPutFromFile) Name() string {
	return "prologic/bitcask"
}

func (t *benchPrologicBitcaskPutFromFile) Setup(testdir string, enable bool) error {
	db, err := bitcask.Open(testdir, bitcask.WithSync(enable), bitcask.WithMaxValueSize(512*1024))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchPrologicBitcaskPutFromFile) Teardown() {
	t.db.Close()
}

func (t *benchPrologicBitcaskPutFromFile) Put(key []byte, file *os.File) error {
	value, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	return t.db.Put(key, value)
}

type benchPrologicBitcaskMerge struct {
	db *bitcask.Bitcask
}

func (t *benchPrologicBitcaskMerge) Name() string {
	return "prologic/bitcask"
}

func (t *benchPrologicBitcaskMerge) Setup(testdir string) error {
	db, err := bitcask.Open(testdir, bitcask.WithMaxValueSize(512*1024), bitcask.WithMaxDatafileSize(100*1024*1024))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchPrologicBitcaskMerge) Teardown() {
	t.db.Close()
}

func (t *benchPrologicBitcaskMerge) LoadData(value []byte, putCount int) error {
	for i := 0; i < putCount; i += 1 {
		if err := t.db.Put([]byte(fmt.Sprintf("%d", i)), value); err != nil {
			return err
		}
	}
	return nil
}

func (t *benchPrologicBitcaskMerge) Merge() error {
	return t.db.Merge()
}

type benchPrologicBitcaskMergeThroughput struct {
	db *bitcask.Bitcask
}

func (t *benchPrologicBitcaskMergeThroughput) Name() string {
	return "prologic/bitcask"
}

func (t *benchPrologicBitcaskMergeThroughput) Setup(testdir string, datafilesize int) error {
	db, err := bitcask.Open(testdir, bitcask.WithMaxValueSize(512*1024), bitcask.WithMaxDatafileSize(datafilesize))
	if err != nil {
		return err
	}
	t.db = db
	return nil
}

func (t *benchPrologicBitcaskMergeThroughput) Teardown() {
	t.db.Close()
}

func (t *benchPrologicBitcaskMergeThroughput) Put(key, value []byte) time.Duration {
	start := time.Now()
	_ = t.db.Put(key, value)
	return time.Since(start)
}

func (t *benchPrologicBitcaskMergeThroughput) Get(key []byte) time.Duration {
	start := time.Now()
	_, _ = t.db.Get(key)
	return time.Since(start)
}

func (t *benchPrologicBitcaskMergeThroughput) Merge() error {
	return t.db.Merge()
}

// ----- }}} prologic/bitcask

func benchGet(b *testing.B, tt benchmarkTestCase, middleware benchMiddlewareGet) {
	testdir, err := os.MkdirTemp("", "bitcask_bench*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	b.SetBytes(int64(tt.size))

	key := []byte("foo")
	value := []byte(strings.Repeat("@", tt.size))

	if err := middleware.Setup(testdir, key, value); err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		ok, err := middleware.Get(key, value)
		if err != nil {
			b.Fatalf("%+v", err)
		}
		if ok != true {
			b.Errorf("unexpected value")
		}
	}
	b.StopTimer()
	middleware.Teardown()
}

func benchPut(b *testing.B, withSync bool, tt benchmarkTestCase, middleware benchMiddlewarePut) {
	testdir, err := os.MkdirTemp("", "bitcask_bench*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	b.SetBytes(int64(tt.size))

	if err := middleware.Setup(testdir, withSync); err != nil {
		b.Fatalf("%+v", err)
	}

	key := []byte("foo")
	value := []byte(strings.Repeat("@", tt.size))

	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		if err := middleware.Put(key, value); err != nil {
			b.Fatalf("%+v", err)
		}
	}
	b.StopTimer()
	middleware.Teardown()
}

func benchPutFromFile(b *testing.B, withSync bool, tt benchmarkTestCase, middleware benchMiddlewarePutFromFile) {
	testdir, err := os.MkdirTemp("", "bitcask_bench*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	b.SetBytes(int64(tt.size))

	file, err := os.CreateTemp("", "bitcask_data*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(file.Name())

	if err := middleware.Setup(testdir, withSync); err != nil {
		b.Fatalf("%+v", err)
	}

	key := []byte("foo")
	value := []byte(strings.Repeat("@", tt.size))

	n, err := file.Write(value)
	if err != nil {
		b.Fatal(err)
	}
	if n < tt.size {
		b.Fatalf("data write size %d < %d", n, tt.size)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		file.Seek(0, io.SeekStart)

		if err := middleware.Put(key, file); err != nil {
			b.Fatalf("%+v", err)
		}
	}
	b.StopTimer()
	middleware.Teardown()
}

func benchMerge(b *testing.B, tt benchmarkTestCase, middleware benchMiddlewareMerge) {
	testdir, err := os.MkdirTemp("", "bitcask_bench*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	b.SetBytes(int64(tt.size))

	valueSize := 128 * 1024
	value := []byte(strings.Repeat("@", valueSize))

	if err := middleware.Setup(testdir); err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		b.StopTimer()
		if err := middleware.LoadData(value, tt.size/valueSize); err != nil {
			b.Fatalf("%+v", err)
		}
		b.StartTimer()
		if err := middleware.Merge(); err != nil {
			b.Fatalf("%+v", err)
		}
	}
	b.StopTimer()
	middleware.Teardown()
}

func benchMergeThroughput(b *testing.B, tt benchmarkTestCase, middleware benchMiddlewareMergeThroughput) {
	testdir, err := os.MkdirTemp("", "bitcask_bench*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	valueSize := 128 * 1024
	value := []byte(strings.Repeat("@", valueSize))

	if err := middleware.Setup(testdir, tt.size); err != nil {
		b.Fatalf("%+v", err)
	}

	N := 500
	b.ResetTimer()
	putSpeed := make([]time.Duration, N+3)
	getSpeed := make([]time.Duration, N+3)
	for i := 0; i < N; i += 3 {
		wg := new(sync.WaitGroup)
		wg.Add(6)
		go func(w *sync.WaitGroup, id int) {
			defer w.Done()

			key := []byte(strconv.Itoa(id))
			dur := middleware.Put(key, value)
			putSpeed[id] = dur
		}(wg, i)
		go func(w *sync.WaitGroup, id int) {
			defer w.Done()

			key := []byte(strconv.Itoa(id + 1))
			dur := middleware.Put(key, value)
			putSpeed[id+1] = dur
		}(wg, i)
		go func(w *sync.WaitGroup, id int) {
			defer w.Done()

			key := []byte(strconv.Itoa(id + 2))
			dur := middleware.Put(key, value)
			putSpeed[id+2] = dur
		}(wg, i)

		go func(w *sync.WaitGroup, id int) {
			defer w.Done()

			time.Sleep(50 * time.Millisecond)
			key := []byte(strconv.Itoa(id))
			dur := middleware.Get(key)
			getSpeed[id] = dur
		}(wg, i)
		go func(w *sync.WaitGroup, id int) {
			defer w.Done()

			time.Sleep(50 * time.Millisecond)
			key := []byte(strconv.Itoa(id + 1))
			dur := middleware.Get(key)
			getSpeed[id+1] = dur
		}(wg, i)
		go func(w *sync.WaitGroup, id int) {
			defer w.Done()

			time.Sleep(50 * time.Millisecond)
			key := []byte(strconv.Itoa(id + 2))
			dur := middleware.Get(key)
			getSpeed[id+2] = dur
		}(wg, i)

		if err := middleware.Merge(); err != nil {
			b.Fatalf("%+v", err)
		}

		b.StopTimer()
		wg.Wait()
		b.StartTimer()
	}
	b.StopTimer()
	middleware.Teardown()

	stat := func(durs []time.Duration) string {
		size := len(durs)
		sum := time.Duration(0)
		for _, dur := range durs {
			sum += dur
		}
		sort.Slice(durs, func(i, j int) bool {
			return durs[i] < durs[j]
		})
		min := durs[0]
		avg := time.Duration(float64(sum) / float64(size))
		max := durs[len(durs)-1]
		p50 := durs[int(float64(size)*0.50)]
		p90 := durs[int(float64(size)*0.90)]
		p95 := durs[int(float64(size)*0.95)]
		p99 := durs[int(float64(size)*0.99)]
		return fmt.Sprintf(
			"min/avg/max/p50/p90/p95/p99 = %s/%s/%s/%s/%s/%s/%s",
			min,
			avg,
			max,
			p50,
			p90,
			p95,
			p99,
		)
	}
	b.Logf("Put %s", stat(putSpeed))
	b.Logf("Get %s", stat(getSpeed))
}

func BenchmarkGet(b *testing.B) {
	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"4K", 4096},
		{"8K", 8192},
		{"32K", 32768},
		{"128K", 131072},
		{"256K", 262144},
		{"512K", 524288},
	}

	benchmarkMiddleware := []factoryBenchMiddlewareGet{
		func() benchMiddlewareGet { return new(benchPrologicBitcaskGet) },
		func() benchMiddlewareGet { return new(benchOctu0bitcaskdbGet) },
	}

	for _, f := range benchmarkMiddleware {
		for _, tt := range tests {
			middleware := f()
			testName := fmt.Sprintf("%s/%s", middleware.Name(), tt.name)
			b.Run(testName, func(tb *testing.B) {
				benchGet(tb, tt, middleware)
			})
		}
	}
}

func BenchmarkPut(b *testing.B) {
	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"4K", 4096},
		{"8K", 8192},
		{"32K", 32768},
		{"128K", 131072},
		{"256K", 262144},
		{"512K", 524288},
	}

	benchmarkMiddleware := []factoryBenchMiddlewarePut{
		func() benchMiddlewarePut { return new(benchPrologicBitcaskPut) },
		func() benchMiddlewarePut { return new(benchOctu0bitcaskdbPut) },
	}

	withSync := []bool{
		false,
		true,
	}

	for _, enable := range withSync {
		withSyncLabel := "WithNosync"
		if enable {
			withSyncLabel = "WithSync"
		}
		for _, f := range benchmarkMiddleware {
			for _, tt := range tests {
				middleware := f()
				testName := fmt.Sprintf("%s/%s/%s", middleware.Name(), withSyncLabel, tt.name)
				b.Run(testName, func(tb *testing.B) {
					benchPut(tb, enable, tt, middleware)
				})
			}
		}
	}
}

func BenchmarkPutFromFile(b *testing.B) {
	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"4K", 4096},
		{"8K", 8192},
		{"32K", 32768},
		{"128K", 131072},
		{"256K", 262144},
		{"512K", 524288},
	}

	benchmarkMiddleware := []factoryBenchMiddlewarePutFromFile{
		func() benchMiddlewarePutFromFile { return new(benchPrologicBitcaskPutFromFile) },
		func() benchMiddlewarePutFromFile { return new(benchOctu0bitcaskdbPutFromFile) },
	}

	withSync := []bool{
		false,
		true,
	}

	for _, enable := range withSync {
		withSyncLabel := "WithNosync"
		if enable {
			withSyncLabel = "WithSync"
		}
		for _, f := range benchmarkMiddleware {
			for _, tt := range tests {
				middleware := f()
				testName := fmt.Sprintf("%s/%s/%s", middleware.Name(), withSyncLabel, tt.name)
				b.Run(testName, func(tb *testing.B) {
					benchPutFromFile(tb, enable, tt, middleware)
				})
			}
		}
	}
}

func BenchmarkMerge(b *testing.B) {
	tests := []benchmarkTestCase{
		{"8MB", 8388608},
		{"16MB", 16777216},
		{"64MB", 67108864},
		{"128MB", 134217728},
		{"256MB", 268435456},
	}

	benchmarkMiddleware := []factoryBenchMiddlewareMerge{
		func() benchMiddlewareMerge { return new(benchPrologicBitcaskMerge) },
		func() benchMiddlewareMerge { return new(benchOctu0bitcaskdbMerge) },
	}

	for _, f := range benchmarkMiddleware {
		for _, tt := range tests {
			middleware := f()
			testName := fmt.Sprintf("%s/%s", middleware.Name(), tt.name)
			b.Run(testName, func(tb *testing.B) {
				benchMerge(tb, tt, middleware)
			})
		}
	}
}

func BenchmarkMergeThroughput(b *testing.B) {
	tests := []benchmarkTestCase{
		{"4MB", 4194304},
		{"8MB", 8388608},
		{"16MB", 16777216},
		{"32MB", 33554432},
	}

	benchmarkMiddleware := []factoryBenchMiddlewareMergeThroughput{
		func() benchMiddlewareMergeThroughput { return new(benchPrologicBitcaskMergeThroughput) },
		func() benchMiddlewareMergeThroughput { return new(benchOctu0bitcaskdbMergeThroughput) },
	}

	for _, f := range benchmarkMiddleware {
		for _, tt := range tests {
			middleware := f()
			testName := fmt.Sprintf("%s/%s", middleware.Name(), tt.name)
			b.Run(testName, func(tb *testing.B) {
				benchMergeThroughput(tb, tt, middleware)
			})
		}
	}
}
