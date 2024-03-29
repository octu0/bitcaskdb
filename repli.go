package bitcaskdb

import (
	"io"
	"time"

	"github.com/octu0/priorate"
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

func (s *repliSource) CurrentFileID() datafile.FileID {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	return s.b.curr.FileID()
}

func (s *repliSource) FileIds() []datafile.FileID {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	ids := make([]datafile.FileID, 0, len(s.b.datafiles)+1)
	for fileID, _ := range s.b.datafiles {
		ids = append(ids, fileID)
	}
	ids = append(ids, s.b.curr.FileID())
	return ids
}

func (s *repliSource) LastIndex(fileID datafile.FileID) int64 {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	if s.b.curr.FileID().Equal(fileID) {
		return s.b.curr.Size()
	}
	df, ok := s.b.datafiles[fileID]
	if ok != true {
		return -1
	}
	return df.Size()
}

func (s *repliSource) Header(fileID datafile.FileID, index int64) (*datafile.Header, datafile.EOFType, error) {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	if s.b.curr.FileID().Equal(fileID) {
		return s.b.curr.ReadAtHeader(index)
	}
	return s.b.datafiles[fileID].ReadAtHeader(index)
}

func (s *repliSource) Read(fileID datafile.FileID, index int64, size int64) (*datafile.Entry, error) {
	s.b.mu.RLock()
	defer s.b.mu.RUnlock()

	if s.b.curr.FileID().Equal(fileID) {
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

func (d *repliDestination) SetCurrentFileID(fileID datafile.FileID) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()

	if d.b.curr.FileID().Equal(fileID) {
		return nil
	}
	prevFileID, err := d.b.closeCurrentFile()
	if err != nil {
		return errors.WithStack(err)
	}
	if err := d.b.openWritableFile(fileID); err != nil {
		return errors.Wrapf(err, "failed open writable:%s prev:%s", fileID, prevFileID)
	}
	return nil
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

func (d *repliDestination) Insert(fileID datafile.FileID, index int64, checksum uint32, key []byte, r io.Reader, expiry time.Time) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()

	df, newOpen, err := d.datafileOpen(fileID)
	if err != nil {
		return errors.WithStack(err)
	}
	if newOpen {
		defer df.Close()
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

func (d *repliDestination) datafileOpen(fileID datafile.FileID) (datafile.Datafile, bool, error) {
	if d.b.curr.FileID().Equal(fileID) {
		return d.b.curr, false, nil
	}

	df, err := datafile.Open(fileID, d.b.path,
		datafile.RuntimeContext(d.b.opt.RuntimeContext),
		datafile.FileMode(d.b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(d.b.opt.TempDir),
		datafile.CopyTempThreshold(d.b.opt.CopyTempThreshold),
	)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return df, true, nil
}

func (d *repliDestination) datafileOpenRead(fileID datafile.FileID) (datafile.Datafile, bool, error) {
	if d.b.curr.FileID().Equal(fileID) {
		return d.b.curr, false, nil
	}
	if df, ok := d.b.datafiles[fileID]; ok {
		return df, false, nil
	}

	df, err := datafile.OpenReadonly(fileID, d.b.path,
		datafile.RuntimeContext(d.b.opt.RuntimeContext),
		datafile.FileMode(d.b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(d.b.opt.TempDir),
		datafile.CopyTempThreshold(d.b.opt.CopyTempThreshold),
	)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return df, true, nil
}

func (d *repliDestination) Delete(key []byte) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()

	return d.b.deleteLocked(key)
}

func (d *repliDestination) merge(dst, src datafile.Datafile, f indexer.Filer) (int64, int64, error) {
	e, err := src.ReadAt(f.Index, f.Size)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	defer e.Close()

	index, size, err := dst.Write(e.Key, e.Value, e.Expiry)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	newFiler := indexer.Filer{
		FileID: dst.FileID(),
		Index:  index,
		Size:   size,
	}
	d.b.trie.Insert(e.Key, newFiler)
	return index, size, nil
}

func (d *repliDestination) mergeDatafile(mergedFiler []indexer.MergeFiler) error {
	d.b.mu.Lock()
	defer d.b.mu.Unlock()

	newFiles := make(map[datafile.FileID]datafile.Datafile, len(mergedFiler))
	for _, mf := range mergedFiler {
		newDF, _, err := d.datafileOpen(mf.FileID)
		if err != nil {
			return errors.WithStack(err)
		}
		newFiles[mf.FileID] = newDF
	}

	oldFiles := make(map[datafile.FileID]datafile.Datafile, len(mergedFiler))
	for _, mf := range mergedFiler {
		oldDF, _, err := d.datafileOpenRead(mf.Filer.FileID)
		if err != nil {
			return errors.WithStack(err)
		}
		oldFiles[mf.Filer.FileID] = oldDF
	}

	for _, mf := range mergedFiler {
		newDF := newFiles[mf.FileID]
		oldDF := oldFiles[mf.Filer.FileID]
		if _, _, err := d.merge(newDF, oldDF, mf.Filer); err != nil {
			return errors.WithStack(err)
		}
	}
	for _, df := range newFiles {
		if err := df.Sync(); err != nil {
			return errors.WithStack(err)
		}
		d.b.datafiles[df.FileID()] = df
	}

	removeFiles := make([]string, 0, len(oldFiles))
	for _, df := range oldFiles {
		delete(d.b.datafiles, df.FileID())
		df.Close()
		removeFiles = append(removeFiles, df.Name())
	}
	if err := removeFileSlowly(removeFiles, priorate.InfLimiter()); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *repliDestination) Merge(mergedFiler []indexer.MergeFiler) error {
	if err := d.mergeDatafile(mergedFiler); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func newRepliDestination(b *Bitcask) *repliDestination {
	return &repliDestination{b}
}
