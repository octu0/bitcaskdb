package datafile

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/codec"
)

type EOFType bool

const (
	IsEOF    EOFType = true
	IsNotEOF EOFType = false
)

var (
	_ Datafile = (*defaultDatafile)(nil)
)

// Datafile is an interface  that represents a readable and writeable datafile
type Datafile interface {
	FileID() FileID
	Name() string
	Close() error
	Sync() error
	Size() int64
	Read() (*Entry, error)
	ReadAt(index, size int64) (*Entry, error)
	ReadAtHeader(index int64) (*Header, EOFType, error)
	Write(key []byte, value io.Reader, expiry time.Time) (int64, int64, error)
}

type defaultDatafile struct {
	sync.RWMutex

	opt    *datafileOpt
	id     FileID
	r      *os.File
	w      *os.File
	offset int64
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (df *defaultDatafile) FileID() FileID {
	return df.id
}

func (df *defaultDatafile) Name() string {
	return df.r.Name()
}

func (df *defaultDatafile) Close() error {
	defer func() {
		df.r.Close()

		df.dec.Close()
		df.enc.Close()
	}()

	// Readonly datafile -- Nothing further to close on the write side
	if df.w == nil {
		return nil
	}

	if err := df.Sync(); err != nil {
		return err
	}

	return df.w.Close()
}

func (df *defaultDatafile) Sync() error {
	if df.w == nil {
		return nil
	}
	df.enc.Flush()
	return df.w.Sync()
}

func (df *defaultDatafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()

	return df.offset
}

// Read reads the next entry from the datafile
func (df *defaultDatafile) Read() (*Entry, error) {
	df.Lock()
	defer df.Unlock()

	p, err := df.dec.Decode()
	if err != nil {
		return nil, err
	}

	e := &Entry{
		Key:       p.Key,
		Value:     p.Value,
		TotalSize: p.N,
		ValueSize: p.ValueSize,
		Checksum:  p.Checksum,
		Expiry:    p.Expiry,
		release: func() {
			p.Close()
		},
	}
	e.setFinalizer()
	return e, nil
}

func (df *defaultDatafile) sectionReader(index, size int64) *io.SectionReader {
	df.RLock()
	defer df.RUnlock()

	return io.NewSectionReader(df.r, index, size)
}

// openSectionReader returns *io.SectionReader with new file open
func (df *defaultDatafile) openSectionReader(index, size int64) (*io.SectionReader, *os.File, error) {
	df.RLock()
	defer df.RUnlock()

	r, err := os.Open(df.r.Name())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return io.NewSectionReader(r, index, size), r, nil
}

func (df *defaultDatafile) ReadAtHeader(index int64) (*Header, EOFType, error) {
	r := df.sectionReader(index, codec.HeaderSize)
	d := codec.NewDecoder(df.opt.ctx, r)
	defer d.Close()

	h, err := d.DecodeHeader()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, IsEOF, nil
		}
		return nil, IsEOF, errors.WithStack(err)
	}

	header := &Header{
		KeySize:   h.KeySize,
		ValueSize: h.ValueSize,
		Checksum:  h.Checksum,
		Expiry:    h.Expiry,
		TotalSize: h.N,
	}

	if df.offset <= (index + header.TotalSize) {
		return header, IsEOF, nil
	}
	return header, IsNotEOF, nil
}

// ReadAt the entry located at index offset with expected serialized size
func (df *defaultDatafile) ReadAt(index, size int64) (*Entry, error) {
	r, f, err := df.openSectionReader(index, size)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	d := codec.NewDecoder(df.opt.ctx, r)
	defer d.Close()

	p, err := d.Decode()
	if err != nil {
		f.Close()
		return nil, errors.WithStack(err)
	}

	e := &Entry{
		Key:       p.Key,
		Value:     p.Value,
		TotalSize: p.N,
		ValueSize: p.ValueSize,
		Checksum:  p.Checksum,
		Expiry:    p.Expiry,
		release: func() {
			p.Close()
			f.Close()
		},
	}
	e.setFinalizer()
	return e, nil
}

func (df *defaultDatafile) Write(key []byte, value io.Reader, expiry time.Time) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, errors.WithStack(errReadonly)
	}

	df.Lock()
	defer df.Unlock()

	prevOffset := df.offset

	size, err := df.enc.Encode(key, value, expiry)
	if err != nil {
		return -1, 0, errors.WithStack(err)
	}
	df.offset += size

	return prevOffset, size, nil
}

func OpenReadonly(id FileID, dir string, funcs ...datafileOptFunc) (*defaultDatafile, error) {
	opts := append(funcs, FileMode(os.FileMode(0400)), readonly(true))
	return open(id, dir, opts...)
}

func Open(id FileID, dir string, funcs ...datafileOptFunc) (*defaultDatafile, error) {
	opts := append(funcs, readonly(false))
	return open(id, dir, opts...)
}

func openWrite(path string, opt *datafileOpt) (*os.File, error) {
	if opt.readonly {
		return nil, nil
	}

	w, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, opt.fileMode)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return w, nil
}

func openRead(path string, opt *datafileOpt) (*os.File, os.FileInfo, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	stat, err := r.Stat()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return r, stat, nil
}

// NewDatafile opens an existing datafile
func open(id FileID, dir string, funcs ...datafileOptFunc) (*defaultDatafile, error) {
	opt := new(datafileOpt)
	for _, fn := range funcs {
		fn(opt)
	}
	initDatafileOpt(opt)

	if id.IsZero() {
		id = NextFileID()
	}

	path := formatDatafilePath(dir, id)

	w, err := openWrite(path, opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r, stat, err := openRead(path, opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &defaultDatafile{
		opt:    opt,
		id:     id,
		r:      r,
		w:      w,
		offset: stat.Size(),
		dec:    codec.NewDecoder(opt.ctx, r),
		enc:    codec.NewEncoder(opt.ctx, w, opt.tempDir, opt.copyTempThreshold),
	}, nil
}
