package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	"github.com/prologic/bitcask/internal/codec"
	"github.com/prologic/bitcask/internal/model"
)

const (
	DefaultDatafileFilename = "%09d.data"
)

var (
	ErrReadonly  = errors.New("error: read only datafile")
	ErrReadError = errors.New("error: read error")

	mxMemPool sync.RWMutex
)

type Datafile struct {
	sync.RWMutex

	id     int
	r      *os.File
	ra     *mmap.ReaderAt
	w      *os.File
	offset int64
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func NewDatafile(path string, id int, readonly bool) (*Datafile, error) {
	var (
		r   *os.File
		ra  *mmap.ReaderAt
		w   *os.File
		err error
	)

	fn := filepath.Join(path, fmt.Sprintf(DefaultDatafileFilename, id))

	if !readonly {
		w, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
		if err != nil {
			return nil, err
		}
	}

	r, err = os.Open(fn)
	if err != nil {
		return nil, err
	}
	stat, err := r.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error calling Stat()")
	}

	ra, err = mmap.Open(fn)
	if err != nil {
		return nil, err
	}

	offset := stat.Size()

	dec := codec.NewDecoder(r)
	enc := codec.NewEncoder(w)

	return &Datafile{
		id:     id,
		r:      r,
		ra:     ra,
		w:      w,
		offset: offset,
		dec:    dec,
		enc:    enc,
	}, nil
}

func (df *Datafile) FileID() int {
	return df.id
}

func (df *Datafile) Name() string {
	return df.r.Name()
}

func (df *Datafile) Close() error {
	defer func() {
		df.ra.Close()
		df.r.Close()
	}()

	// Readonly Datafile -- Nothing further to close on the write side
	if df.w == nil {
		return nil
	}

	err := df.Sync()
	if err != nil {
		return err
	}
	return df.w.Close()
}

func (df *Datafile) Sync() error {
	if df.w == nil {
		return nil
	}
	return df.w.Sync()
}

func (df *Datafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

func (df *Datafile) Read() (e model.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()

	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

func (df *Datafile) ReadAt(index, size int64) (e model.Entry, err error) {
	var n int

	b := make([]byte, size)

	if df.w == nil {
		n, err = df.ra.ReadAt(b, index)
	} else {
		n, err = df.r.ReadAt(b, index)
	}
	if err != nil {
		return
	}
	if int64(n) != size {
		err = ErrReadError
		return
	}

	valueOffset, _ := codec.GetKeyValueSizes(b)
	codec.DecodeWithoutPrefix(b[codec.KeySize+codec.ValueSize:], valueOffset, &e)

	return
}

func (df *Datafile) Write(e model.Entry) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, ErrReadonly
	}

	df.Lock()
	defer df.Unlock()

	e.Offset = df.offset

	n, err := df.enc.Encode(e)
	if err != nil {
		return -1, 0, err
	}
	df.offset += n

	return e.Offset, n, nil
}