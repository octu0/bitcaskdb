package data

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/prologic/bitcask/internal"
	"github.com/prologic/bitcask/internal/data/codec"
	"golang.org/x/exp/mmap"
)

const (
	defaultDatafileFilename = "%09d.data"
)

var (
	errReadonly  = errors.New("error: read only datafile")
	errReadError = errors.New("error: read error")
)

// Datafile is an interface  that represents a readable and writeable datafile
type Datafile interface {
	FileID() int
	Name() string
	Close() error
	Sync() error
	Size() int64
	Read() (internal.Entry, int64, error)
	ReadAt(index, size int64) (internal.Entry, error)
	Write(internal.Entry) (int64, int64, error)
}

type datafile struct {
	sync.RWMutex

	id           int
	r            *os.File
	ra           *mmap.ReaderAt
	w            *os.File
	offset       int64
	dec          *codec.Decoder
	enc          *codec.Encoder
	maxKeySize   uint32
	maxValueSize uint64
}

// NewDatafile opens an existing datafile
func NewDatafile(path string, id int, readonly bool, maxKeySize uint32, maxValueSize uint64, fileMode os.FileMode) (Datafile, error) {
	var (
		r   *os.File
		ra  *mmap.ReaderAt
		w   *os.File
		err error
	)

	fn := filepath.Join(path, fmt.Sprintf(defaultDatafileFilename, id))

	if !readonly {
		w, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, fileMode)
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

	dec := codec.NewDecoder(r, maxKeySize, maxValueSize)
	enc := codec.NewEncoder(w)

	return &datafile{
		id:           id,
		r:            r,
		ra:           ra,
		w:            w,
		offset:       offset,
		dec:          dec,
		enc:          enc,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}, nil
}

func (df *datafile) FileID() int {
	return df.id
}

func (df *datafile) Name() string {
	return df.r.Name()
}

func (df *datafile) Close() error {
	defer func() {
		df.ra.Close()
		df.r.Close()
	}()

	// Readonly datafile -- Nothing further to close on the write side
	if df.w == nil {
		return nil
	}

	err := df.Sync()
	if err != nil {
		return err
	}
	return df.w.Close()
}

func (df *datafile) Sync() error {
	if df.w == nil {
		return nil
	}
	return df.w.Sync()
}

func (df *datafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

// Read reads the next entry from the datafile
func (df *datafile) Read() (e internal.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()

	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

// ReadAt the entry located at index offset with expected serialized size
func (df *datafile) ReadAt(index, size int64) (e internal.Entry, err error) {
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
		err = errReadError
		return
	}

	codec.DecodeEntry(b, &e, df.maxKeySize, df.maxValueSize)

	return
}

func (df *datafile) Write(e internal.Entry) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, errReadonly
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
