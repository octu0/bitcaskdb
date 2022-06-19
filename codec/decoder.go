package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/runtime"
)

type ReaderAtSeeker interface {
	io.Seeker
	io.ReaderAt
}

// Decoder wraps an underlying io.Reader and allows you to stream
// Entry decodings on it.
type Decoder struct {
	ctx           runtime.Context
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

func (d *Decoder) DecodeHeader() (*Header, error) {
	bytePool := d.ctx.Buffer().BytePool()
	head := bytePool.Get()
	defer bytePool.Put(head)

	if _, err := d.r.ReadAt(head[:HeaderSize], d.offset); err != nil {
		return nil, errors.Wrap(err, "failed reading head")
	}

	h := bytes.NewReader(head[:HeaderSize])

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
	return &Header{
		KeySize:   int32(keySize),
		ValueSize: int64(valueSize),
		Checksum:  checksum,
		Expiry:    expiry,
		N:         HeaderSize + int64(keySize) + int64(valueSize),
	}, nil
}

// Decode decodes the next Entry from the current stream
func (d *Decoder) Decode() (*Payload, error) {
	bytePool := d.ctx.Buffer().BytePool()
	header, err := d.DecodeHeader()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	key := bytePool.Get()
	if int32(cap(key)) < header.KeySize {
		bytePool.Put(key)
		key = make([]byte, header.KeySize)
	}
	if _, err := d.r.ReadAt(key[:header.KeySize], d.offset+HeaderSize); err != nil {
		return nil, errors.Wrapf(err, "failed reading key data size=%d", header.KeySize)
	}

	valueReader, done := d.createValueReader(header.KeySize, header.ValueSize)

	releaseFn := func() {
		bytePool.Put(key)
		done()
	}
	p := &Payload{
		Key:       key[:header.KeySize],
		Value:     valueReader,
		Checksum:  header.Checksum,
		Expiry:    header.Expiry,
		N:         header.N,
		ValueSize: header.ValueSize,
		release:   releaseFn,
	}
	p.setFinalizer()
	d.r.Seek(p.N, io.SeekCurrent)
	d.offset += p.N
	return p, nil
}

func (d *Decoder) createValueReader(k int32, v int64) (io.Reader, func()) {
	if v < d.valueOnMemory {
		return d.createBufferedValueReader(k, v)
	}
	return d.createDefaultValueReader(k, v)
}

func (d *Decoder) createBufferedValueReader(k int32, v int64) (io.Reader, func()) {
	value := io.NewSectionReader(d.r, d.offset+HeaderSize+int64(k), v)

	bufferPool := d.ctx.Buffer().BufferPool()
	buf := bufferPool.Get()
	buf.ReadFrom(value)
	return buf, func() {
		bufferPool.Put(buf)
	}
}

func (d *Decoder) createDefaultValueReader(k int32, v int64) (io.Reader, func()) {
	value := io.NewSectionReader(d.r, d.offset+HeaderSize+int64(k), v)

	bfrPool := d.ctx.Buffer().BufioReaderPool()
	buffered := bfrPool.Get(value)
	return buffered, func() {
		bfrPool.Put(buffered)
	}
}

// NewDecoder creates a streaming Entry decoder.
func NewDecoder(ctx runtime.Context, r ReaderAtSeeker, valueOnMemory int64) *Decoder {
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
