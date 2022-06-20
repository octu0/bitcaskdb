package bitcaskdb

import (
	"io"
	"time"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
	"github.com/octu0/bitcaskdb/repli"
)

var (
	_ repli.Source      = (*repliSource)(nil)
	_ repli.Destination = (*repliDestination)(nil)
)

type repliSource struct {
	b *Bitcask
}

func (s *repliSource) FileIds() []int32 {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	ids := make([]int32, 0, len(s.b.datafiles)+1)
	for fileID, _ := range s.b.datafiles {
		ids = append(ids, fileID)
	}
	ids = append(ids, s.b.curr.FileID())
	return ids
}

func (s *repliSource) LastIndex(fileID int32) int64 {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	if s.b.curr.FileID() == fileID {
		return s.b.curr.Size()
	}
	return s.b.datafiles[fileID].Size()
}

func (s *repliSource) Header(fileID int32, index int64) (*datafile.Header, bool, error) {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	if s.b.curr.FileID() == fileID {
		return s.b.curr.ReadAtHeader(index)
	}
	return s.b.datafiles[fileID].ReadAtHeader(index)
}

func (s *repliSource) Read(fileID int32, index int64, size int64) (*datafile.Entry, error) {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	if s.b.curr.FileID() == fileID {
		return s.b.curr.ReadAt(index, size)
	}
	return s.b.datafiles[fileID].ReadAt(index, size)
}

func newRepliSource(b *Bitcask) *repliSource {
	return &repliSource{b}
}

type repliDestination struct {
	b *Bitcask
}

func (d *repliDestination) LastFiles() []repli.FileIDAndIndex {
	d.b.mu.RLock()
	defer d.b.mu.RUnlock()

	files := make([]repli.FileIDAndIndex, 0, len(d.b.datafiles)+1)
	files = append(files, repli.FileIDAndIndex{
		FileID: d.b.curr.FileID(),
		Index:  d.b.curr.Size(),
	})
	for fileID, df := range d.b.datafiles {
		files = append(files, repli.FileIDAndIndex{
			FileID: fileID,
			Index:  df.Size(),
		})
	}
	return files
}

func (d *repliDestination) Insert(fileID int32, index int64, checksum uint32, key []byte, r io.Reader, expiry time.Time) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()

	df, err := d.datafileOpen(fileID)
	if err != nil {
		return errors.WithStack(err)
	}
	index, size, err := df.Write(key, r, expiry)
	if err != nil {
		return errors.WithStack(err)
	}
	d.b.trie.Insert(key, indexer.Filer{
		FileID: fileID,
		Index:  index,
		Size:   size,
	})
	if expiry.IsZero() != true {
		d.b.ttlIndex.Insert(key, expiry)
	}
	return d.b.maybeRotate()
}

func (d *repliDestination) datafileOpen(fileID int32) (datafile.Datafile, error) {
	if d.b.curr.FileID() == fileID {
		return d.b.curr, nil
	}

	return datafile.Open(
		datafile.RuntimeContext(d.b.opt.RuntimeContext),
		datafile.Path(d.b.path),
		datafile.FileID(fileID),
		datafile.FileMode(d.b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(d.b.opt.TempDir),
		datafile.CopyTempThreshold(d.b.opt.CopyTempThreshold),
		datafile.ValueOnMemoryThreshold(d.b.opt.ValueOnMemoryThreshold),
	)
}

func (d *repliDestination) Delete(key []byte) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()

	return d.b.delete(key)
}

func newRepliDestination(b *Bitcask) *repliDestination {
	return &repliDestination{b}
}
