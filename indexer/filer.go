package indexer

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/runtime"
)

const (
	FilerByteSize int = datafile.FileIDByteSize + 8 + 8
)

// Filer represents the location of the value on disk. This is used by the
// internal Adaptive Radix Tree to hold an in-memory structure mapping keys to
// locations on disk of where the value(s) can be read from.
type Filer struct {
	FileID datafile.FileID
	Index  int64
	Size   int64
}

func writeFilerIndex(ctx runtime.Context, w io.Writer, t art.Tree) error {
	pool := ctx.Buffer().BufioWriterPool()
	bfw := pool.Get(w)
	defer pool.Put(bfw)

	var lastErr error
	t.ForEach(func(node art.Node) bool {
		if err := writeKeyBytes(ctx, bfw, node.Key()); err != nil {
			lastErr = errors.WithStack(err)
			return false
		}

		f := node.Value().(Filer)
		if err := writeFiler(bfw, f); err != nil {
			lastErr = errors.WithStack(err)
			return false
		}
		return true
	})
	if lastErr != nil {
		return errors.WithStack(lastErr)
	}
	if err := bfw.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ReadIndex reads a persisted from a io.Reader into a Tree
func readFilerIndex(ctx runtime.Context, r io.Reader, t art.Tree) error {
	for {
		key, releaseFn, err := readKeyBytes(ctx, r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		defer releaseFn()

		f, err := readFiler(r)
		if err != nil {
			return errors.WithStack(err)
		}

		t.Insert(key, f)
	}
	return nil
}

func writeFiler(w io.Writer, f Filer) error {
	if err := binary.Write(w, binary.BigEndian, uint64(f.FileID.Time)); err != nil {
		return errors.Wrap(err, "failed writing FileID.Time")
	}
	if err := binary.Write(w, binary.BigEndian, uint64(f.FileID.Rand)); err != nil {
		return errors.Wrap(err, "failed writing FileID.Rand")
	}
	if err := binary.Write(w, binary.BigEndian, uint64(f.Index)); err != nil {
		return errors.Wrap(err, "failed writing index")
	}
	if err := binary.Write(w, binary.BigEndian, uint64(f.Size)); err != nil {
		return errors.Wrap(err, "failed writing size")
	}
	return nil
}

func readFiler(r io.Reader) (Filer, error) {
	fileIDTime := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &fileIDTime); err != nil {
		return Filer{}, errors.Wrap(err, "failed reading FileID.Time")
	}
	fileIDRand := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &fileIDRand); err != nil {
		return Filer{}, errors.Wrap(err, "failed reading FileID.Rand")
	}
	index := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &index); err != nil {
		return Filer{}, errors.Wrap(err, "failed reading index")
	}
	size := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return Filer{}, errors.Wrap(err, "failed reading size")
	}

	return Filer{
		FileID: datafile.CreateFileID(int64(fileIDTime), int64(fileIDRand)),
		Index:  int64(index),
		Size:   int64(size),
	}, nil
}

type filerIndex struct {
	ctx runtime.Context
}

func (i *filerIndex) Load(path string) (art.Tree, bool, error) {
	t := art.New()

	if _, err := os.Stat(path); err != nil {
		// not found
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
