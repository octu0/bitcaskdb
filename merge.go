package bitcaskdb

import (
	"os"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"sync"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
)

const (
	mergeDirPattern string = "merge*"
)

type merger struct {
	mutex   *sync.RWMutex
	merging bool
}

func (m *merger) isMerging() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.merging
}

func (m *merger) Merge(b *Bitcask) error {
	if m.isMerging() {
		return errors.WithStack(ErrMergeInProgress)
	}

	m.mutex.Lock()
	m.merging = true
	m.mutex.Unlock()

	defer func() {
		m.mutex.Lock()
		m.merging = false
		m.mutex.Unlock()
	}()

	mergeFileIds, err := m.forwardCurrentDafafile(b)
	if err != nil {
		return errors.WithStack(err)
	}

	temp, err := m.renewMergedDB(b, mergeFileIds)
	if err != nil {
		return errors.WithStack(err)
	}
	defer temp.Destroy()

	// no reads and writes till we reopen
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.close(); err != nil {
		// try recovery
		if err2 := b.reopen(); err2 != nil {
			return errors.Wrapf(err2, "failed close() / reopen() cause:%+v", err)
		}
		return errors.Wrap(err, "failed to close()")
	}

	if err := m.moveMerged(b, temp.DBPath()); err != nil {
		return errors.WithStack(err)
	}

	b.metadata.ReclaimableSpace = 0

	// And finally reopen the database
	if err := b.reopen(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *merger) forwardCurrentDafafile(b *Bitcask) ([]int32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentFileID, err := b.closeCurrentFile()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mergeFileIds := make([]int32, 0, len(b.datafiles))
	for id, _ := range b.datafiles {
		mergeFileIds = append(mergeFileIds, id)
	}

	if err := b.openWritableFile(currentFileID + 1); err != nil {
		return nil, errors.WithStack(err)
	}

	sort.Slice(mergeFileIds, func(i, j int) bool {
		return mergeFileIds[i] < mergeFileIds[j]
	})

	return mergeFileIds, nil
}

func (m *merger) renewMergedDB(b *Bitcask, mergeFileIds []int32) (*mergeTempDB, error) {
	temp, err := openMergeTempDB(b.path, b.opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := temp.Merge(b, mergeFileIds); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := temp.SyncAndClose(); err != nil {
		return nil, errors.WithStack(err)
	}
	return temp, nil
}

func (m *merger) moveMerged(b *Bitcask, mergedDBPath string) error {
	currentFileID := b.curr.FileID()

	if err := m.removeArchive(b, currentFileID); err != nil {
		return errors.WithStack(err)
	}
	if err := m.moveDBFiles(b, mergedDBPath); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (m *merger) removeArchive(b *Bitcask, currentFileID int32) error {
	files, err := os.ReadDir(b.path)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if filename == lockfile {
			continue
		}

		ids, err := datafile.ParseIds([]string{filename})
		if err != nil {
			return errors.WithStack(err)
		}

		if 0 < len(ids) {
			fileID := ids[0]
			// keep current
			if currentFileID == fileID {
				continue
			}
		}

		path := filepath.Join(b.path, filename)
		if err := os.Remove(path); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (m *merger) moveDBFiles(b *Bitcask, fromDBPath string) error {
	// Rename all merged data files
	files, err := os.ReadDir(fromDBPath)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, file := range files {
		filename := file.Name()
		// see https://git.mills.io/prologic/bitcask/issues/225
		if filename == lockfile {
			continue
		}
		fromFile := filepath.Join(fromDBPath, filename)
		toFile := filepath.Join(b.path, filename)
		if err := os.Rename(fromFile, toFile); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func newMerger() *merger {
	return &merger{
		mutex:   new(sync.RWMutex),
		merging: false,
	}
}

type mergeTempDB struct {
	tempDir string
	mdb     *Bitcask
}

func (t *mergeTempDB) DBPath() string {
	return t.mdb.path
}

func (t *mergeTempDB) DB() *Bitcask {
	return t.mdb
}

// Rewrite all key/value pairs into merged database
// Doing this automatically strips deleted keys and
// old key/value pairs
func (t *mergeTempDB) Merge(src *Bitcask, mergeFileIds []int32) error {
	t.mdb.mu.Lock()
	defer t.mdb.mu.Unlock()

	m := make(map[int32]datafile.Datafile, len(mergeFileIds))
	datafiles := make([]datafile.Datafile, len(mergeFileIds))
	for i, fileID := range mergeFileIds {
		df, err := datafile.OpenReadonly(
			datafile.RuntimeContext(src.opt.RuntimeContext),
			datafile.Path(src.path),
			datafile.FileID(fileID),
			datafile.TempDir(src.opt.TempDir),
			datafile.CopyTempThreshold(src.opt.CopyTempThreshold),
		)
		if err != nil {
			return errors.WithStack(err)
		}
		datafiles[i] = df
		m[fileID] = df
	}
	defer func() {
		for _, df := range datafiles {
			df.Close()
		}
	}()

	for _, df := range datafiles {
		if err := loadIndexFromDatafile(t.mdb.trie, t.mdb.ttlIndex, df); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := t.mergeDatafileLocked(m); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (t *mergeTempDB) mergeDatafileLocked(m map[int32]datafile.Datafile) error {
	var lastErr error
	t.mdb.trie.ForEach(func(node art.Node) bool {
		filer := node.Value().(indexer.Filer)
		e, err := m[filer.FileID].ReadAt(filer.Index, filer.Size)
		if err != nil {
			lastErr = errors.WithStack(err)
			return false
		}
		defer e.Close()

		// no merge expired
		if isExpiredFromTime(e.Expiry) {
			return true
		}
		if err := t.mdb.putAndIndexLocked(e.Key, e.Value, e.Expiry); err != nil {
			lastErr = errors.WithStack(err)
			return false
		}
		return true
	})
	if lastErr != nil {
		return errors.WithStack(lastErr)
	}
	return nil
}

func (t *mergeTempDB) SyncAndClose() error {
	if err := t.mdb.Sync(); err != nil {
		return errors.WithStack(err)
	}
	if err := t.mdb.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (t *mergeTempDB) Destroy() {
	goruntime.SetFinalizer(t, nil) // clear
	os.RemoveAll(t.tempDir)
}

func finalizeMergeTempDB(t *mergeTempDB) {
	t.Destroy()
}

func openMergeTempDB(basedir string, opt *option) (*mergeTempDB, error) {
	tempDir, err := os.MkdirTemp(basedir, mergeDirPattern)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	mdb, err := Open(tempDir, withMerge(opt))
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, errors.WithStack(err)
	}
	t := &mergeTempDB{tempDir, mdb}
	goruntime.SetFinalizer(t, finalizeMergeTempDB)
	return t, nil
}
