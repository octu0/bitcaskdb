package codec

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"

	"github.com/octu0/bitcaskdb/runtime"
)

const (
	defaultTempDataTruncateThreshold int64         = 100 * 1024 * 1024
	truncateInterval                 time.Duration = 100 * time.Millisecond
)

// Memory management is handled by sync.Pool,
// instead of shared by runtime.Buffer, since large bytes.Buffer may be allocated.
var (
	tempMemoryPool = &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 1*1024*1024)) // 1 MB
		},
	}
)

type temporaryData struct {
	ctx               runtime.Context
	memory            *bytes.Buffer
	file              *os.File
	written           int64
	closed            bool
	tempDir           string
	tempFileThreshold int64
	truncateThreshold int64
}

func (t *temporaryData) writeToMemory(p []byte) (int, error) {
	size, err := t.memory.Write(p)
	if err != nil {
		return 0, err
	}

	t.written += int64(size)
	if t.tempFileThreshold < t.written {
		f, err := os.CreateTemp(t.tempDir, "copy")
		if err != nil {
			return 0, err
		}
		if _, err := t.memory.WriteTo(f); err != nil {
			f.Close()
			return 0, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return 0, err
		}
		t.file = f
	}
	return size, nil
}

func (t *temporaryData) writeToFile(p []byte) (int, error) {
	size, err := t.file.Write(p)
	if err != nil {
		return 0, err
	}
	if err := t.file.Sync(); err != nil {
		return 0, err
	}
	t.written += int64(size)
	return size, nil
}

func (t *temporaryData) Write(p []byte) (int, error) {
	if t.file != nil {
		return t.writeToFile(p)
	}
	return t.writeToMemory(p)
}

func (t *temporaryData) WriteTo(w io.Writer) (int64, error) {
	pool := t.ctx.Buffer().BytePool()
	buf := pool.Get()
	defer pool.Put(buf)

	if t.file != nil {
		t.file.Seek(0, io.SeekStart)
		return io.CopyBuffer(w, t.file, buf)
	}
	return io.CopyBuffer(w, t.memory, buf)
}

func (t *temporaryData) closeFile() error {
	// remove file slowly
	if t.truncateThreshold < t.written {
		truncateCount := (t.written - 1) / t.truncateThreshold
		for i := int64(0); i < truncateCount; i += 1 {
			nextSize := t.truncateThreshold * (truncateCount - i)
			t.file.Truncate(nextSize)
			time.Sleep(truncateInterval)
		}
	}
	return os.Remove(t.file.Name())
}

func (t *temporaryData) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true

	t.memory.Reset()
	tempMemoryPool.Put(t.memory)

	if t.file != nil {
		return t.closeFile()
	}
	return nil
}

func newTemopraryData(ctx runtime.Context, tempDir string, threshold int64) *temporaryData {
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	if threshold < 1 {
		threshold = int64(ctx.Buffer().BufferSize())
	}
	return &temporaryData{
		ctx:               ctx,
		memory:            tempMemoryPool.Get().(*bytes.Buffer),
		file:              nil,
		written:           0,
		closed:            false,
		tempDir:           tempDir,
		tempFileThreshold: threshold,
		truncateThreshold: defaultTempDataTruncateThreshold,
	}
}
