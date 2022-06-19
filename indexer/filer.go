package indexer

import (
	"os"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/runtime"
	"github.com/octu0/bitcaskdb/util"
)

type filerIndex struct {
	ctx runtime.Context
}

func (i *filerIndex) Load(path string) (art.Tree, bool, error) {
	t := art.New()

	if util.Exists(path) != true {
		return t, false, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return t, true, errors.WithStack(err)
	}
	defer f.Close()

	if err := readFilerIndex(i.ctx, f, t); err != nil {
		return t, true, errors.WithStack(err)
	}
	return t, true, nil
}

func (i *filerIndex) Save(t art.Tree, path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := writeFilerIndex(i.ctx, f, t); err != nil {
		f.Close()
		return errors.WithStack(err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return errors.WithStack(err)
	}

	if err := f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func NewFilerIndexer(ctx runtime.Context) *filerIndex {
	return &filerIndex{ctx}
}
