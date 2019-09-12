package data

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/prologic/bitcask/internal"
)

const (
	keySize      = 4
	valueSize    = 8
	checksumSize = 4
)

var (
	// ErrInvalidKeyOrValueSize indicates a serialized key/value size
	// which is greater than specified limit
	ErrInvalidKeyOrValueSize = errors.New("key/value size is invalid")
)

// NewEncoder creates a streaming Entry encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: bufio.NewWriter(w)}
}

// Encoder wraps an underlying io.Writer and allows you to stream
// Entry encodings on it.
type Encoder struct {
	w *bufio.Writer
}

// Encode takes any Entry and streams it to the underlying writer.
// Messages are framed with a key-length and value-length prefix.
func (e *Encoder) Encode(msg internal.Entry) (int64, error) {
	var bufKeyValue = make([]byte, keySize+valueSize)
	binary.BigEndian.PutUint32(bufKeyValue[:keySize], uint32(len(msg.Key)))
	binary.BigEndian.PutUint64(bufKeyValue[keySize:keySize+valueSize], uint64(len(msg.Value)))
	if _, err := e.w.Write(bufKeyValue); err != nil {
		return 0, errors.Wrap(err, "failed writing key & value length prefix")
	}

	if _, err := e.w.Write(msg.Key); err != nil {
		return 0, errors.Wrap(err, "failed writing key data")
	}
	if _, err := e.w.Write(msg.Value); err != nil {
		return 0, errors.Wrap(err, "failed writing value data")
	}

	bufChecksumSize := bufKeyValue[:checksumSize]
	binary.BigEndian.PutUint32(bufChecksumSize, msg.Checksum)
	if _, err := e.w.Write(bufChecksumSize); err != nil {
		return 0, errors.Wrap(err, "failed writing checksum data")
	}

	if err := e.w.Flush(); err != nil {
		return 0, errors.Wrap(err, "failed flushing data")
	}

	return int64(keySize + valueSize + len(msg.Key) + len(msg.Value) + checksumSize), nil
}

// NewDecoder creates a streaming Entry decoder.
func NewDecoder(r io.Reader, maxKeySize uint32, maxValueSize uint64) *Decoder {
	return &Decoder{
		r:            r,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}
}

// Decoder wraps an underlying io.Reader and allows you to stream
// Entry decodings on it.
type Decoder struct {
	r            io.Reader
	maxKeySize   uint32
	maxValueSize uint64
}

// Decode decodes the next Entry from the current stream
func (d *Decoder) Decode(v *internal.Entry) (int64, error) {
	prefixBuf := make([]byte, keySize+valueSize)

	_, err := io.ReadFull(d.r, prefixBuf)
	if err != nil {
		return 0, err
	}

	actualKeySize, actualValueSize, err := getKeyValueSizes(prefixBuf, d.maxKeySize, d.maxValueSize)
	if err != nil {
		return 0, errors.Wrap(err, "error while getting key/value serialized sizes")
	}

	buf := make([]byte, uint64(actualKeySize)+actualValueSize+checksumSize)
	if _, err = io.ReadFull(d.r, buf); err != nil {
		return 0, errors.Wrap(translateError(err), "failed reading saved data")
	}

	decodeWithoutPrefix(buf, actualKeySize, v)
	return int64(keySize + valueSize + uint64(actualKeySize) + actualValueSize + checksumSize), nil
}

// DecodeEntry decodes a serialized entry
func DecodeEntry(b []byte, e *internal.Entry, maxKeySize uint32, maxValueSize uint64) error {
	valueOffset, _, err := getKeyValueSizes(b, maxKeySize, maxValueSize)
	if err != nil {
		return errors.Wrap(err, "key/value sizes are invalid")
	}

	decodeWithoutPrefix(b[keySize+valueSize:], valueOffset, e)

	return nil
}

func getKeyValueSizes(buf []byte, maxKeySize uint32, maxValueSize uint64) (uint32, uint64, error) {
	actualKeySize := binary.BigEndian.Uint32(buf[:keySize])
	actualValueSize := binary.BigEndian.Uint64(buf[keySize:])

	if actualKeySize > maxKeySize || actualValueSize > maxValueSize {
		return 0, 0, ErrInvalidKeyOrValueSize
	}

	return actualKeySize, actualValueSize, nil
}

func decodeWithoutPrefix(buf []byte, valueOffset uint32, v *internal.Entry) {
	v.Key = buf[:valueOffset]
	v.Value = buf[valueOffset : len(buf)-checksumSize]
	v.Checksum = binary.BigEndian.Uint32(buf[len(buf)-checksumSize:])
}

func translateError(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
