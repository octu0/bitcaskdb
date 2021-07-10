package index

import (
	"encoding/binary"
	"io"
	"os"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
	"git.mills.io/prologic/bitcask/internal"
)

type ttlIndexer struct{}

func NewTTLIndexer() Indexer {
	return ttlIndexer{}
}

func (i ttlIndexer) Save(t art.Tree, path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	buf := make([]byte, int64Size)
	for it := t.Iterator(); it.HasNext(); {
		node, err := it.Next()
		if err != nil {
			return err
		}
		// save key
		err = writeBytes(node.Key(), f)
		if err != nil {
			return err
		}
		// save key ttl
		binary.BigEndian.PutUint64(buf, uint64(node.Value().(time.Time).Unix()))
		_, err = f.Write(buf)
		if err != nil {
			return err
		}
	}
	return f.Sync()
}

func (i ttlIndexer) Load(path string, maxKeySize uint32) (art.Tree, bool, error) {
	t := art.New()
	if !internal.Exists(path) {
		return t, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return t, true, err
	}
	buf := make([]byte, int64Size)
	for {
		key, err := readKeyBytes(f, maxKeySize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return t, true, err
		}
		_, err = io.ReadFull(f, buf)
		if err != nil {
			return t, true, err
		}
		expiry := time.Unix(int64(binary.BigEndian.Uint64(buf)), 0).UTC()
		t.Insert(key, expiry)
	}
	return t, true, nil
}
