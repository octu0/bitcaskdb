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
	ctx    *context.Context
	m      *sync.RWMutex
	r      ReaderAtSeeker
	offset int64
	closed bool
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

	if _, err := d.r.ReadAt(head[:MetaInfoSize], d.offset); err != nil {
		return nil, errors.Wrap(err, "failed reading head")
	}

	h := bytes.NewReader(head[:MetaInfoSize])

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
	if _, err := d.r.ReadAt(key[:keySize], d.offset+MetaInfoSize); err != nil {
		return nil, errors.Wrapf(err, "failed reading key data size=%d", keySize)
	}

	k := int64(keySize)
	v := int64(valueSize)
	value := io.NewSectionReader(d.r, d.offset+MetaInfoSize+k, v)

	bfrPool := d.ctx.Buffer().BufioReaderPool()
	buffered := bfrPool.Get(value)

	releaseFn := func() {
		bytePool.Put(key)
		bfrPool.Put(buffered)
	}
	p := &Payload{
		Key:       key[:keySize],
		Value:     buffered,
		Checksum:  checksum,
		Expiry:    expiry,
		N:         MetaInfoSize + k + v,
		ValueSize: v,
		release:   releaseFn,
	}
	p.setFinalizer()
	d.r.Seek(p.N, io.SeekCurrent)
	d.offset += p.N
	return p, nil
}

// NewDecoder creates a streaming Entry decoder.
func NewDecoder(ctx *context.Context, r ReaderAtSeeker) *Decoder {
	return &Decoder{
		ctx:    ctx,
		m:      new(sync.RWMutex),
		r:      r,
		offset: int64(0),
		closed: false,
	}
}
