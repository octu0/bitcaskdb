package codec

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/context"
)

const (
	keySize      = 4 // uint32
	valueSize    = 8 // uint64
	checksumSize = 4 // uint32
	ttlSize      = 8 // uint64
	MetaInfoSize = keySize + valueSize + checksumSize + ttlSize
)

const (
	tempDataTruncateThreshold int64         = 100 * 1024 * 1024
	truncateInterval          time.Duration = 100 * time.Millisecond
)

var (
	errInvalidKeyOrValueSize = errors.New("key/value size is invalid")
	errNegativeRead          = errors.New("negative count from io.Read")
	errTemporaryFileNotOpen  = errors.New("temporaty file is not open")
	errKeySizeTooLarge       = errors.New("key size too large")
)

var (
	_ io.ReadCloser = (*Payload)(nil)
)

type Payload struct {
	Key       []byte
	Value     io.Reader
	Checksum  uint32
	Expiry    time.Time
	N         int64
	ValueSize int64
	closed    bool
	release   func()
}

func (p *Payload) setFinalizer() {
	runtime.SetFinalizer(p, finalizePayload)
}

func (p *Payload) Read(buf []byte) (int, error) {
	return p.Value.Read(buf)
}

func (p *Payload) Close() error {
	if p.closed != true {
		p.closed = true
		runtime.SetFinalizer(p, nil) // clear finalizer
		if p.release != nil {
			p.release()
		}
	}
	return nil
}

func finalizePayload(p *Payload) {
	runtime.SetFinalizer(p, nil) // clear finalizer
	p.Close()
}

type temporaryData struct {
	ctx       *context.Context
	memory    *bytes.Buffer
	file      *os.File
	written   int64
	closed    bool
	tempDir   string
	threshold int64
}

func (t *temporaryData) writeToMemory(p []byte) (int, error) {
	size, err := t.memory.Write(p)
	if err != nil {
		return 0, err
	}

	t.written += int64(size)
	if t.threshold < t.written {
		f, err := os.CreateTemp(t.tempDir, "copy")
		if err != nil {
			return 0, err
		}
		if _, err := t.memory.WriteTo(f); err != nil {
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
	if t.file != nil {
		pool := t.ctx.Buffer().BytePool()
		buf := pool.Get()
		defer pool.Put(buf)

		return io.CopyBuffer(w, t.file, buf)
	}
	return t.memory.WriteTo(w)
}

func (t *temporaryData) closeFile() error {
	// remove file slowly
	if tempDataTruncateThreshold < t.written {
		truncateCount := (t.written - 1) / tempDataTruncateThreshold
		for i := int64(0); i < truncateCount; i += 1 {
			nextSize := tempDataTruncateThreshold * (truncateCount - i)
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

	t.ctx.Buffer().BufferPool().Put(t.memory)

	if t.file != nil {
		return t.closeFile()
	}
	return nil
}

func newTemopraryData(ctx *context.Context, tempDir string, threshold int64) *temporaryData {
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	return &temporaryData{
		ctx:       ctx,
		memory:    ctx.Buffer().BufferPool().Get(),
		file:      nil,
		written:   0,
		closed:    false,
		tempDir:   tempDir,
		threshold: threshold,
	}
}

func IsCorruptedData(err error) bool {
	if errors.Is(err, errInvalidKeyOrValueSize) {
		return true
	}
	if errors.Is(err, errNegativeRead) {
		return true
	}
	if errors.Is(err, errKeySizeTooLarge) {
		return true
	}
	return false
}
