package index

import (
	"os"

	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
)

// Indexer is an interface for loading and saving the index (an Adaptive Radix Tree)
type Indexer interface {
	Load(path string, maxkeySize uint32) (art.Tree, bool, error)
	Save(t art.Tree, path string) error
}

// NewIndexer returns an instance of the default `Indexer` implemtnation
// which perists the index (an Adaptive Radix Tree) as a binary blob on file
func NewIndexer() Indexer {
	return &indexer{}
}

type indexer struct{}

func (i *indexer) Load(path string, maxKeySize uint32) (art.Tree, bool, error) {
	t := art.New()

	if !internal.Exists(path) {
		return t, false, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return t, true, err
	}
	defer f.Close()

	if err := readIndex(f, t, maxKeySize); err != nil {
		return t, true, err
	}
	return t, true, nil
}

func (i *indexer) Save(t art.Tree, path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := writeIndex(t, f); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return f.Close()
}
