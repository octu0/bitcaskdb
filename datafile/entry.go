package datafile

import (
	"hash/crc32"
	"io"
	"runtime"
	"time"

	"github.com/octu0/bitcaskdb/context"
)

var (
	_ io.ReadCloser = (*Entry)(nil)
)

// Entry represents a key/value in the database
type Entry struct {
	Key       []byte
	Value     io.Reader
	TotalSize int64
	ValueSize int64
	Checksum  uint32
	Expiry    time.Time
	closed    bool
	release   func()
}

func (e *Entry) setFinalizer() {
	runtime.SetFinalizer(e, finalizeEntry)
}

func (e *Entry) Read(p []byte) (int, error) {
	return e.Value.Read(p)
}

func (e *Entry) Close() error {
	if e.closed != true {
		e.closed = true
		runtime.SetFinalizer(e, nil) // clear finalizer
		if e.release != nil {
			e.release()
		}
	}
	return nil
}

func (e *Entry) Validate(ctx *context.Context) error {
	pool := ctx.Buffer().BytePool()
	buf := pool.Get()
	defer pool.Put(buf)

	c := crc32.New(crc32.IEEETable)
	for {
		n, err := e.Value.Read(buf)
		if n < 0 {
			return errNegativeRead
		}
		if err == io.EOF {
			if c.Sum32() != e.Checksum {
				return ErrChecksumFailed
			}
			return nil
		}
		if _, err := c.Write(buf[:n]); err != nil {
			return err
		}
	}
}

func finalizeEntry(e *Entry) {
	runtime.SetFinalizer(e, nil) // clear finalizer
	e.Close()
}
