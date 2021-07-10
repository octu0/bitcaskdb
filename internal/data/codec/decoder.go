package codec

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/pkg/errors"
	"git.mills.io/prologic/bitcask/internal"
)

var (
	errInvalidKeyOrValueSize = errors.New("key/value size is invalid")
	errCantDecodeOnNilEntry  = errors.New("can't decode on nil entry")
	errTruncatedData         = errors.New("data is truncated")
)

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
	if v == nil {
		return 0, errCantDecodeOnNilEntry
	}

	prefixBuf := make([]byte, keySize+valueSize)

	_, err := io.ReadFull(d.r, prefixBuf)
	if err != nil {
		return 0, err
	}

	actualKeySize, actualValueSize, err := getKeyValueSizes(prefixBuf, d.maxKeySize, d.maxValueSize)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, uint64(actualKeySize)+actualValueSize+checksumSize+ttlSize)
	if _, err = io.ReadFull(d.r, buf); err != nil {
		return 0, errTruncatedData
	}

	decodeWithoutPrefix(buf, actualKeySize, v)
	return int64(keySize + valueSize + uint64(actualKeySize) + actualValueSize + checksumSize + ttlSize), nil
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

	if (maxKeySize > 0 && actualKeySize > maxKeySize) || (maxValueSize > 0 && actualValueSize > maxValueSize) || actualKeySize == 0 {

		return 0, 0, errInvalidKeyOrValueSize
	}

	return actualKeySize, actualValueSize, nil
}

func decodeWithoutPrefix(buf []byte, valueOffset uint32, v *internal.Entry) {
	v.Key = buf[:valueOffset]
	v.Value = buf[valueOffset : len(buf)-checksumSize-ttlSize]
	v.Checksum = binary.BigEndian.Uint32(buf[len(buf)-checksumSize-ttlSize : len(buf)-ttlSize])
	v.Expiry = getKeyExpiry(buf)
}

func getKeyExpiry(buf []byte) *time.Time {
	expiry := binary.BigEndian.Uint64(buf[len(buf)-ttlSize:])
	if expiry == uint64(0) {
		return nil
	}
	t := time.Unix(int64(expiry), 0).UTC()
	return &t
}

// IsCorruptedData indicates if the error correspondes to possible data corruption
func IsCorruptedData(err error) bool {
	switch err {
	case errCantDecodeOnNilEntry, errInvalidKeyOrValueSize, errTruncatedData:
		return true
	default:
		return false
	}
}
