package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/context"
)

type ReaderAtSeeker interface {
	io.Seeker
	io.ReaderAt
}

// Decoder wraps an underlying io.Reader and allows you to stream
// Entry decodings on it.
type Decoder struct {
	ctx           *context.Context
	m             *sync.RWMutex
	r             ReaderAtSeeker
	valueOnMemory int64
	offset        int64
	closed        bool
}

func (d *Decoder) Close() {
	if d.closed {
		return
	}
	d.closed = true
}

// Decode decodes the next Entry from the current stream
func (d *Decoder) Decode() (*Payload, error) {
	bytePool := d.ctx.Buffer().BytePool()
	head := bytePool.Get()
	defer bytePool.Put(head)

	if _, err := d.r.ReadAt(head[:headerSize], d.offset); err != nil {
		return nil, errors.Wrap(err, "failed reading head")
	}

	h := bytes.NewReader(head[:headerSize])

	keySize := uint32(0)
	if err := binary.Read(h, binary.BigEndian, &keySize); err != nil {
		return nil, errors.Wrap(err, "failed reading key prefix")
	}
	if keySize < 1 {
		return nil, errInvalidKeyOrValueSize
	}
	valueSize := uint64(0)
	if err := binary.Read(h, binary.BigEndian, &valueSize); err != nil {
		return nil, errors.Wrap(err, "failed reading value prefix")
	}

	checksum := uint32(0)
	if err := binary.Read(h, binary.BigEndian, &checksum); err != nil {
		return nil, errors.Wrap(err, "failed reading checksum data")
	}
	ttl := uint64(0)
	if err := binary.Read(h, binary.BigEndian, &ttl); err != nil {
		return nil, errors.Wrap(err, "failed reading ttl data")
	}
	expiry := time.Time{}
	if 0 < ttl {
		expiry = time.Unix(int64(ttl), 0).UTC()
	}

	key := bytePool.Get()
	if uint32(cap(key)) < keySize {
		bytePool.Put(key)
		key = make([]byte, keySize)
	}
	if _, err := d.r.ReadAt(key[:keySize], d.offset+headerSize); err != nil {
		return nil, errors.Wrapf(err, "failed reading key data size=%d", keySize)
	}

	k := int64(keySize)
	v := int64(valueSize)

	valueReader, done := d.createValueReader(k, v)

	releaseFn := func() {
		bytePool.Put(key)
		done()
	}
	p := &Payload{
		Key:       key[:keySize],
		Value:     valueReader,
		Checksum:  checksum,
		Expiry:    expiry,
		N:         headerSize + k + v,
		ValueSize: v,
		release:   releaseFn,
	}
	p.setFinalizer()
	d.r.Seek(p.N, io.SeekCurrent)
	d.offset += p.N
	return p, nil
}

func (d *Decoder) createValueReader(k int64, v int64) (io.Reader, func()) {
	if v < d.valueOnMemory {
		return d.createBufferedValueReader(k, v)
	}
	return d.createDefaultValueReader(k, v)
}

func (d *Decoder) createBufferedValueReader(k int64, v int64) (io.Reader, func()) {
	value := io.NewSectionReader(d.r, d.offset+headerSize+k, v)

	bufferPool := d.ctx.Buffer().BufferPool()
	buf := bufferPool.Get()
	buf.ReadFrom(value)
	return buf, func() {
		bufferPool.Put(buf)
	}
}

func (d *Decoder) createDefaultValueReader(k int64, v int64) (io.Reader, func()) {
	value := io.NewSectionReader(d.r, d.offset+headerSize+k, v)

	bfrPool := d.ctx.Buffer().BufioReaderPool()
	buffered := bfrPool.Get(value)
	return buffered, func() {
		bfrPool.Put(buffered)
	}
}

// NewDecoder creates a streaming Entry decoder.
func NewDecoder(ctx *context.Context, r ReaderAtSeeker, valueOnMemory int64) *Decoder {
	if valueOnMemory < 1 {
		valueOnMemory = int64(ctx.Buffer().BufferSize())
	}
	return &Decoder{
		ctx:           ctx,
		m:             new(sync.RWMutex),
		r:             r,
		valueOnMemory: valueOnMemory,
		offset:        int64(0),
		closed:        false,
	}
}
