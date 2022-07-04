package indexer

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/runtime"
)

type releaseFunc func()

func writeKeyBytes(ctx runtime.Context, w io.Writer, key []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(key))); err != nil {
		return errors.Wrap(err, "failed writing key prefix")
	}
	if _, err := w.Write(key); err != nil {
		return errors.Wrap(err, "failed writing key data")
	}
	return nil
}

func readKeyBytes(ctx runtime.Context, r io.Reader) ([]byte, releaseFunc, error) {
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
		pool.Put(key)
		key = make([]byte, size)
	}

	if _, err := r.Read(key[:size]); err != nil {
		return nil, nil, errors.Wrap(err, "failed reading data")
	}
	releaseFn := func() {
		pool.Put(key)
	}
	return key[:size], releaseFn, nil
}
