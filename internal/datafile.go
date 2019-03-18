package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"

	pb "github.com/prologic/bitcask/internal/proto"
	"github.com/prologic/bitcask/internal/streampb"
)

const (
	DefaultDatafileFilename = "%09d.data"
)

var (
	ErrReadonly = errors.New("error: read only datafile")
)

type Datafile struct {
	sync.RWMutex

	id     int
	r      *os.File
	w      *os.File
	offset int64
	dec    *streampb.Decoder
	enc    *streampb.Encoder
}

func NewDatafile(path string, id int, readonly bool) (*Datafile, error) {
	var (
		r   *os.File
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

	offset := stat.Size()

	dec := streampb.NewDecoder(r)
	enc := streampb.NewEncoder(w)

	return &Datafile{
		id:     id,
		r:      r,
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
	if df.w == nil {
		return df.r.Close()
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

func (df *Datafile) Size() (int64, error) {
	df.RLock()
	defer df.RUnlock()
	return df.offset, nil
}

func (df *Datafile) Read() (e pb.Entry, err error) {
	df.Lock()
	defer df.Unlock()

	return e, df.dec.Decode(&e)
}

func (df *Datafile) ReadAt(index int64) (e pb.Entry, err error) {
	df.Lock()
	defer df.Unlock()

	_, err = df.r.Seek(index, os.SEEK_SET)
	if err != nil {
		return
	}

	return e, df.dec.Decode(&e)
}

func (df *Datafile) Write(e pb.Entry) (int64, error) {
	if df.w == nil {
		return -1, ErrReadonly
	}

	df.Lock()
	defer df.Unlock()

	e.Offset = df.offset

	n, err := df.enc.Encode(&e)
	if err != nil {
		return -1, err
	}
	df.offset += n

	return e.Offset, nil
}
