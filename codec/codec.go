package codec

import (
	"io"
	"runtime"
	"time"
)

const (
	headerKeySize      int64 = 4 // uint32
	headerValueSize    int64 = 8 // uint64
	headerChecksumSize int64 = 4 // uint32
	headerTTLSize      int64 = 8 // uint64
	headerSize         int64 = headerKeySize + headerValueSize + headerChecksumSize + headerTTLSize
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
