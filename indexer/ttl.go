package indexer

import (
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/runtime"
)

func readTTLIndex(ctx runtime.Context, r io.Reader, t art.Tree) error {
	for {
		key, done, err := readKeyBytes(ctx, r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		defer done()

		ttl, err := readTTL(r)
		if err != nil {
			return errors.WithStack(err)
		}

		t.Insert(key, ttl)
	}
	return nil
}

func writeTTLIndex(ctx runtime.Context, w io.Writer, t art.Tree) (lastErr error) {
	pool := ctx.Buffer().BufioWriterPool()
	bfw := pool.Get(w)
	defer pool.Put(bfw)

	t.ForEach(func(node art.Node) bool {
		if err := writeKeyBytes(ctx, bfw, node.Key()); err != nil {
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

type ttlIndexer struct {
	ctx runtime.Context
}

func (i *ttlIndexer) Load(path string) (art.Tree, bool, error) {
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

	if err := readTTLIndex(i.ctx, f, t); err != nil {
		return t, true, errors.WithStack(err)
	}
	return t, true, nil
}

func (i *ttlIndexer) Save(t art.Tree, path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := writeTTLIndex(i.ctx, f, t); err != nil {
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

func NewTTLIndexer(ctx runtime.Context) *ttlIndexer {
	return &ttlIndexer{ctx}
}
