package codec

import (
	"io"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	headerKeySize      int64 = 4                                                                    // uint32
	headerValueSize    int64 = 8                                                                    // uint64
	headerChecksumSize int64 = 4                                                                    // uint32
	headerTTLSize      int64 = 8                                                                    // uint64
	HeaderSize         int64 = headerKeySize + headerValueSize + headerChecksumSize + headerTTLSize // 24 byte
)

var (
	_ io.ReadCloser = (*Payload)(nil)
)

type Header struct {
	KeySize   int32
	ValueSize int64
	Checksum  uint32
	Expiry    time.Time
	N         int64
}

type Payload struct {
	Key       []byte
	Value     io.Reader
	Checksum  uint32
	Expiry    time.Time
	N         int64
	ValueSize int64
	closed    int32
	release   func()
}

func (p *Payload) setFinalizer() {
	runtime.SetFinalizer(p, finalizePayload)
}

func (p *Payload) Read(buf []byte) (int, error) {
	return p.Value.Read(buf)
}

func (p *Payload) Close() error {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		runtime.SetFinalizer(p, nil) // clear finalizer
		if p.release != nil {
			p.release()
		}
	}
	return nil
}

func finalizePayload(p *Payload) {
	p.Close()
}
