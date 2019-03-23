package internal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	pb "github.com/prologic/bitcask/internal/proto"
	"github.com/prologic/bitcask/internal/streampb"
)

const (
	DefaultDatafileFilename = "%09d.data"
)

var (
	ErrReadonly  = errors.New("error: read only datafile")
	ErrReadError = errors.New("error: read error")
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

func (df *Datafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

func (df *Datafile) Read() (e pb.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()

	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

func (df *Datafile) ReadAt(index, size int64) (e pb.Entry, err error) {
	log.WithField("index", index).WithField("size", size).Debug("ReadAt")

	b := make([]byte, size)
	n, err := df.r.ReadAt(b, index)
	if err != nil {
		return
	}
	if int64(n) != size {
		err = ErrReadError
		return
	}

	buf := bytes.NewBuffer(b)
	dec := streampb.NewDecoder(buf)
	_, err = dec.Decode(&e)
	return
}

func (df *Datafile) Write(e pb.Entry) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, ErrReadonly
	}

	df.Lock()
	defer df.Unlock()

	e.Offset = df.offset

	n, err := df.enc.Encode(&e)
	if err != nil {
		return -1, 0, err
	}
	df.offset += n

	return e.Offset, n, nil
}
