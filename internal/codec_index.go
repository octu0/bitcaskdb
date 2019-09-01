package internal

import (
	"encoding/binary"
	"io"

	art "github.com/plar/go-adaptive-radix-tree"
)

const (
	Int32Size  = 4
	Int64Size  = 8
	FileIDSize = Int32Size
	OffsetSize = Int64Size
	SizeSize   = Int64Size
)

func ReadBytes(r io.Reader) ([]byte, error) {
	s := make([]byte, Int32Size)
	_, err := io.ReadFull(r, s)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(s)
	b := make([]byte, size)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func WriteBytes(b []byte, w io.Writer) (int, error) {
	s := make([]byte, Int32Size)
	binary.BigEndian.PutUint32(s, uint32(len(b)))
	n, err := w.Write(s)
	if err != nil {
		return n, err
	}
	m, err := w.Write(b)
	if err != nil {
		return (n + m), err
	}
	return (n + m), nil
}

func ReadItem(r io.Reader) (Item, error) {
	buf := make([]byte, (FileIDSize + OffsetSize + SizeSize))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return Item{}, err
	}

	return Item{
		FileID: int(binary.BigEndian.Uint32(buf[:FileIDSize])),
		Offset: int64(binary.BigEndian.Uint64(buf[FileIDSize:(FileIDSize + OffsetSize)])),
		Size:   int64(binary.BigEndian.Uint64(buf[(FileIDSize + OffsetSize):])),
	}, nil
}

func WriteItem(item Item, w io.Writer) (int, error) {
	buf := make([]byte, (FileIDSize + OffsetSize + SizeSize))
	binary.BigEndian.PutUint32(buf[:FileIDSize], uint32(item.FileID))
	binary.BigEndian.PutUint64(buf[FileIDSize:(FileIDSize+OffsetSize)], uint64(item.Offset))
	binary.BigEndian.PutUint64(buf[(FileIDSize+OffsetSize):], uint64(item.Size))
	n, err := w.Write(buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func ReadIndex(r io.Reader, t art.Tree) error {
	for {
		key, err := ReadBytes(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		item, err := ReadItem(r)
		if err != nil {
			return err
		}

		t.Insert(key, item)
	}

	return nil
}

func WriteIndex(t art.Tree, w io.Writer) (err error) {
	t.ForEach(func(node art.Node) bool {
		_, err = WriteBytes(node.Key(), w)
		if err != nil {
			return false
		}

		item := node.Value().(Item)
		_, err := WriteItem(item, w)
		if err != nil {
			return false
		}

		return true
	})
	return
}
