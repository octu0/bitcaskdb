package indexer

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/context"
)

// Filer represents the location of the value on disk. This is used by the
// internal Adaptive Radix Tree to hold an in-memory structure mapping keys to
// locations on disk of where the value(s) can be read from.
type Filer struct {
	FileID int32 `json:"fileid"`
	Index  int64 `json:"index"`
	Size   int64 `json:"size"`
}

func readKeyBytes(ctx *context.Context, r io.Reader) ([]byte, func(), error) {
	pool := ctx.Buffer().BytePool()

	size := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, nil, errors.Wrap(err, "failed reading size")
	}
	if size < 1 {
		return nil, nil, errors.WithStack(errTruncatedKeySize)
	}

	key := pool.Get()
	if uint32(cap(key)) < size {
		return nil, nil, errors.WithStack(errKeySizeTooLarge)
	}

	if _, err := r.Read(key[:size]); err != nil {
		return nil, nil, errors.Wrap(err, "failed reading data")
	}
	releaseFn := func() {
		pool.Put(key)
	}
	return key[:size], releaseFn, nil
}

func writeBytes(ctx *context.Context, w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "failed writing key prefix")
	}
	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "failed writing key data")
	}
	return nil
}

func readFiler(r io.Reader) (Filer, error) {
	fileID := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &fileID); err != nil {
		return Filer{}, errors.Wrap(err, "failed reading fileID")
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
		FileID: int32(fileID),
		Index:  int64(index),
		Size:   int64(size),
	}, nil
}

func writeFiler(w io.Writer, f Filer) error {
	if err := binary.Write(w, binary.BigEndian, uint32(f.FileID)); err != nil {
		return errors.Wrap(err, "failed writing fileID")
	}
	if err := binary.Write(w, binary.BigEndian, uint64(f.Index)); err != nil {
		return errors.Wrap(err, "failed writing index")
	}
	if err := binary.Write(w, binary.BigEndian, uint64(f.Size)); err != nil {
		return errors.Wrap(err, "failed writing size")
	}
	return nil
}

func readTTL(r io.Reader) (time.Time, error) {
	ttl := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &ttl); err != nil {
		return time.Time{}, errors.Wrap(err, "failed reading ttl")
	}
	expiry := time.Time{}
	if 0 < ttl {
		expiry = time.Unix(int64(ttl), 0).UTC()
	}
	return expiry, nil
}

func writeTTL(w io.Writer, expiry time.Time) error {
	if expiry.IsZero() {
		if err := binary.Write(w, binary.BigEndian, uint64(0)); err != nil {
			return errors.Wrap(err, "failed writing ttl")
		}
	} else {
		if err := binary.Write(w, binary.BigEndian, uint64(expiry.Unix())); err != nil {
			return errors.Wrap(err, "failed writing ttl")
		}
	}
	return nil
}

// ReadIndex reads a persisted from a io.Reader into a Tree
func readFilerIndex(ctx *context.Context, r io.Reader, t art.Tree) error {
	pool := ctx.Buffer().BufioReaderPool()
	bfr := pool.Get(r)
	defer pool.Put(bfr)

	for {
		key, done, err := readKeyBytes(ctx, bfr)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		defer done()

		f, err := readFiler(bfr)
		if err != nil {
			return errors.WithStack(err)
		}

		t.Insert(key, f)
	}
	return nil
}

func writeFilerIndex(ctx *context.Context, w io.Writer, t art.Tree) (lastErr error) {
	pool := ctx.Buffer().BufioWriterPool()
	bfw := pool.Get(w)
	defer pool.Put(bfw)

	t.ForEach(func(node art.Node) bool {
		if err := writeBytes(ctx, bfw, node.Key()); err != nil {
			lastErr = err
			return false
		}

		f := node.Value().(Filer)
		if err := writeFiler(bfw, f); err != nil {
			lastErr = err
			return false
		}
		return true
	})
	if lastErr != nil {
		return lastErr
	}
	if err := bfw.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func readTTLIndex(ctx *context.Context, r io.Reader, t art.Tree) error {
	pool := ctx.Buffer().BufioReaderPool()
	bfr := pool.Get(r)
	defer pool.Put(bfr)

	for {
		key, done, err := readKeyBytes(ctx, bfr)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		defer done()

		ttl, err := readTTL(bfr)
		if err != nil {
			return errors.WithStack(err)
		}

		t.Insert(key, ttl)
	}
	return nil
}

func writeTTLIndex(ctx *context.Context, w io.Writer, t art.Tree) (lastErr error) {
	pool := ctx.Buffer().BufioWriterPool()
	bfw := pool.Get(w)
	defer pool.Put(bfw)

	t.ForEach(func(node art.Node) bool {
		if err := writeBytes(ctx, bfw, node.Key()); err != nil {
			lastErr = err
			return false
		}

		expiry := node.Value().(time.Time)
		if err := writeTTL(bfw, expiry); err != nil {
			lastErr = err
			return false
		}
		return true
	})
	if lastErr != nil {
		return lastErr
	}
	if err := bfw.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
