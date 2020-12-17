package index

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
)

var (
	errTruncatedKeySize = errors.New("key size is truncated")
	errTruncatedKeyData = errors.New("key data is truncated")
	errTruncatedData    = errors.New("data is truncated")
	errKeySizeTooLarge  = errors.New("key size too large")
)

const (
	int32Size  = 4
	int64Size  = 8
	fileIDSize = int32Size
	offsetSize = int64Size
	sizeSize   = int64Size
)

func readKeyBytes(r io.Reader, maxKeySize uint32) ([]byte, error) {
	s := make([]byte, int32Size)
	_, err := io.ReadFull(r, s)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, errors.Wrap(errTruncatedKeySize, err.Error())
	}
	size := binary.BigEndian.Uint32(s)
	if maxKeySize > 0 && size > uint32(maxKeySize) {
		return nil, errKeySizeTooLarge
	}

	b := make([]byte, size)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, errors.Wrap(errTruncatedKeyData, err.Error())
	}
	return b, nil
}

func writeBytes(b []byte, w io.Writer) error {
	s := make([]byte, int32Size)
	binary.BigEndian.PutUint32(s, uint32(len(b)))
	_, err := w.Write(s)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func readItem(r io.Reader) (internal.Item, error) {
	buf := make([]byte, (fileIDSize + offsetSize + sizeSize))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return internal.Item{}, errors.Wrap(errTruncatedData, err.Error())
	}

	return internal.Item{
		FileID: int(binary.BigEndian.Uint32(buf[:fileIDSize])),
		Offset: int64(binary.BigEndian.Uint64(buf[fileIDSize:(fileIDSize + offsetSize)])),
		Size:   int64(binary.BigEndian.Uint64(buf[(fileIDSize + offsetSize):])),
	}, nil
}

func writeItem(item internal.Item, w io.Writer) error {
	buf := make([]byte, (fileIDSize + offsetSize + sizeSize))
	binary.BigEndian.PutUint32(buf[:fileIDSize], uint32(item.FileID))
	binary.BigEndian.PutUint64(buf[fileIDSize:(fileIDSize+offsetSize)], uint64(item.Offset))
	binary.BigEndian.PutUint64(buf[(fileIDSize+offsetSize):], uint64(item.Size))
	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

// ReadIndex reads a persisted from a io.Reader into a Tree
func readIndex(r io.Reader, t art.Tree, maxKeySize uint32) error {
	for {
		key, err := readKeyBytes(r, maxKeySize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		item, err := readItem(r)
		if err != nil {
			return err
		}

		t.Insert(key, item)
	}

	return nil
}

func writeIndex(t art.Tree, w io.Writer) (err error) {
	t.ForEach(func(node art.Node) bool {
		err = writeBytes(node.Key(), w)
		if err != nil {
			return false
		}

		item := node.Value().(internal.Item)
		err := writeItem(item, w)
		return err == nil
	})
	return
}

// IsIndexCorruption returns a boolean indicating whether the error
// is known to report a corruption data issue
func IsIndexCorruption(err error) bool {
	cause := errors.Cause(err)
	switch cause {
	case errKeySizeTooLarge, errTruncatedData, errTruncatedKeyData, errTruncatedKeySize:
		return true
	}
	return false
}
