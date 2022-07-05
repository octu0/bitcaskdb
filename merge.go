package bitcaskdb

import (
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"golang.org/x/time/rate"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
)

const (
	mergeDirPattern  string = "merge*"
	removeParkPrefix string = ".mark.%s"
	snapshotTrieFile string = "snapshot_trie"
)

const (
	defaultTruncateThreshold int64 = 100 * 1024 * 1024
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

func (m *merger) Merge(b *Bitcask, lim *rate.Limiter) error {
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

	lastFileID, mergeFileIds, err := m.forwardCurrentDafafile(b)
	if err != nil {
		return errors.WithStack(err)
	}

	snapshot, err := m.snapshotIndexer(b, lim)
	if err != nil {
		return errors.WithStack(err)
	}
	defer snapshot.Destroy(lim)

	temp, err := m.renewMergedDB(b, mergeFileIds, snapshot, lim)
	if err != nil {
		return errors.WithStack(err)
	}
	defer temp.Destroy(lim)

	// Reduce b blocking time by performing b.mu.Lock/Unlock within merger.reopen()
	removeMarkedFiles, err := m.reopen(b, temp, lastFileID)
	if err != nil {
		return errors.WithStack(err)
	}

	removeFileSlowly(removeMarkedFiles, lim)
	return nil
}

func (m *merger) reopen(b *Bitcask, temp *mergeTempDB, lastFileID datafile.FileID) ([]string, error) {
	// no reads and writes till we reopen
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.closeLocked(); err != nil {
		// try recovery
		if err2 := b.reopenLocked(); err2 != nil {
			return nil, errors.Wrapf(err2, "failed close() / reopen() cause:%+v", err)
		}
		return nil, errors.Wrap(err, "failed to close()")
	}

	removeMarkedFiles, err := m.moveMerged(b, lastFileID, temp.DBPath())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	b.metadata.ReclaimableSpace = 0

	// And finally reopen the database
	if err := b.reopenLocked(); err != nil {
		removeFileSlowly(removeMarkedFiles, nil)
		return nil, errors.WithStack(err)
	}
	return removeMarkedFiles, nil
}

func (m *merger) forwardCurrentDafafile(b *Bitcask) (datafile.FileID, []datafile.FileID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentFileID, err := b.closeCurrentFile()
	if err != nil {
		return datafile.FileID{}, nil, errors.WithStack(err)
	}

	mergeFileIds := make([]datafile.FileID, 0, len(b.datafiles))
	for id, _ := range b.datafiles {
		mergeFileIds = append(mergeFileIds, id)
	}

	if err := b.openWritableFile(datafile.NextFileID()); err != nil {
		return datafile.FileID{}, nil, errors.WithStack(err)
	}

	sort.Slice(mergeFileIds, func(i, j int) bool {
		return mergeFileIds[i].Newer(mergeFileIds[j])
	})

	return currentFileID, mergeFileIds, nil
}

func (m *merger) snapshotIndexer(b *Bitcask, lim *rate.Limiter) (*snapshotTrie, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st, err := openSnapshotTrie(b.opt.TempDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed open snapshot trie")
	}

	var lastErr error
	b.trie.ForEach(func(node art.Node) bool {
		if err := st.Write(node.Key(), node.Value().(indexer.Filer)); err != nil {
			lastErr = errors.WithStack(err)
			return false
		}

		r := lim.ReserveN(time.Now(), indexer.FilerByteSize)
		if r.OK() != true {
			return true
		}

		if d := r.Delay(); 0 < d {
			time.Sleep(d)
		}
		return true
	})
	if lastErr != nil {
		return nil, errors.WithStack(lastErr)
	}
	return st, nil
}

func (m *merger) renewMergedDB(b *Bitcask, mergeFileIds []datafile.FileID, st *snapshotTrie, lim *rate.Limiter) (*mergeTempDB, error) {
	temp, err := openMergeTempDB(b.path, b.opt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := temp.MergeDatafiles(b, mergeFileIds, st, lim); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := temp.SyncAndClose(); err != nil {
		return nil, errors.WithStack(err)
	}
	return temp, nil
}

func (m *merger) moveMerged(b *Bitcask, lastFileID datafile.FileID, mergedDBPath string) ([]string, error) {
	removeMarkedFiles, err := m.markArchive(b, lastFileID)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := m.moveDBFiles(b, mergedDBPath); err != nil {
		return nil, errors.WithStack(err)
	}
	return removeMarkedFiles, nil
}

func (m *merger) markArchive(b *Bitcask, lastFileID datafile.FileID) ([]string, error) {
	files, err := os.ReadDir(b.path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	removeMark := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := file.Name()
		if filename == lockfile {
			continue
		}

		ids := datafile.GrepFileIds([]string{filename})

		if 0 < len(ids) {
			fileID := ids[0]
			// keep newer
			if lastFileID.Newer(fileID) {
				continue
			}
		}

		fromFile := filepath.Join(b.path, filename)
		toFile := filepath.Join(b.path, fmt.Sprintf(removeParkPrefix, filename))
		if err := os.Rename(fromFile, toFile); err != nil {
			return nil, errors.WithStack(err)
		}
		removeMark = append(removeMark, toFile)
	}
	return removeMark, nil
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
		// keep current index
		if filename == filerIndexFile || filename == ttlIndexFile {
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
	tempDir   string
	mdb       *Bitcask
	closed    bool
	destroyed bool
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
func (t *mergeTempDB) MergeDatafiles(src *Bitcask, mergeFileIds []datafile.FileID, st *snapshotTrie, lim *rate.Limiter) error {
	t.mdb.mu.Lock()
	defer t.mdb.mu.Unlock()

	m := make(map[datafile.FileID]datafile.Datafile, len(mergeFileIds))
	defer func() {
		for _, df := range m {
			df.Close()
		}
	}()
	for _, fileID := range mergeFileIds {
		df, err := datafile.OpenReadonly(fileID, src.path,
			datafile.RuntimeContext(src.opt.RuntimeContext),
			datafile.TempDir(src.opt.TempDir),
			datafile.CopyTempThreshold(src.opt.CopyTempThreshold),
		)
		if err != nil {
			return errors.WithStack(err)
		}
		m[fileID] = df
	}

	if err := t.mergeDatafileLocked(st, m, lim); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (t *mergeTempDB) mergeDatafileLocked(st *snapshotTrie, m map[datafile.FileID]datafile.Datafile, lim *rate.Limiter) error {
	return st.ReadAll(func(data snapshotTrieData) error {
		filer := data.Value

		df, ok := m[filer.FileID]
		if ok != true {
			return nil
		}

		e, err := df.ReadAt(filer.Index, filer.Size)
		if err != nil {
			return errors.WithStack(err)
		}
		defer e.Close()

		// no merge expired
		if isExpiredFromTime(e.Expiry) {
			return nil
		}
		if _, _, err := t.mdb.put(e.Key, e.Value, e.Expiry); err != nil {
			return errors.WithStack(err)
		}

		r := lim.ReserveN(time.Now(), int(e.TotalSize))
		if r.OK() != true {
			return nil
		}

		if d := r.Delay(); 0 < d {
			time.Sleep(d)
		}
		return nil
	})
}

func (t *mergeTempDB) SyncAndClose() error {
	if t.closed {
		return nil
	}

	if err := t.mdb.Sync(); err != nil {
		return errors.WithStack(err)
	}
	if err := t.mdb.Close(); err != nil {
		return errors.WithStack(err)
	}
	t.closed = true
	return nil
}

func (t *mergeTempDB) Destroy(lim *rate.Limiter) {
	if t.destroyed {
		return
	}

	goruntime.SetFinalizer(t, nil) // clear
	t.SyncAndClose()

	files, err := filepath.Glob(filepath.Join(t.tempDir, "*"))
	if err != nil {
		os.RemoveAll(t.tempDir)
		t.destroyed = true
		return
	}
	removeFileSlowly(files, lim)
	os.RemoveAll(t.tempDir)
	t.destroyed = true
}

func finalizeMergeTempDB(t *mergeTempDB) {
	t.Destroy(nil)
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
	st := &mergeTempDB{
		tempDir:   tempDir,
		mdb:       mdb,
		closed:    false,
		destroyed: false,
	}
	goruntime.SetFinalizer(st, finalizeMergeTempDB)
	return st, nil
}

type snapshotTrie struct {
	file      *os.File
	enc       *gob.Encoder
	closed    bool
	destroyed bool
}

type snapshotTrieData struct {
	Key   []byte
	Value indexer.Filer
}

func (st *snapshotTrie) Close() {
	if st.closed {
		return
	}
	st.file.Close()
	st.closed = true
}

func (st *snapshotTrie) Write(key []byte, value indexer.Filer) error {
	return st.enc.Encode(snapshotTrieData{
		Key:   key,
		Value: value,
	})
}

func (st *snapshotTrie) ReadAll(fn func(snapshotTrieData) error) error {
	st.file.Seek(0, io.SeekStart)

	dec := gob.NewDecoder(st.file)
	for {
		data := snapshotTrieData{}
		if err := dec.Decode(&data); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		if err := fn(data); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (st *snapshotTrie) Destroy(lim *rate.Limiter) {
	if st.destroyed {
		return
	}

	goruntime.SetFinalizer(st, nil)
	st.Close()
	removeFileSlowly([]string{st.file.Name()}, lim)
	st.destroyed = true
}

func finalizeSnapshotTrie(st *snapshotTrie) {
	st.Destroy(nil)
}

func openSnapshotTrie(tempDir string) (*snapshotTrie, error) {
	f, err := os.CreateTemp(tempDir, snapshotTrieFile)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	st := &snapshotTrie{
		file:      f,
		enc:       gob.NewEncoder(f),
		closed:    false,
		destroyed: false,
	}
	goruntime.SetFinalizer(st, finalizeSnapshotTrie)
	return st, nil
}

func removeFileSlowly(files []string, lim *rate.Limiter) error {
	if lim == nil {
		lim = rate.NewLimiter(rate.Inf, math.MaxInt)
	}

	for _, file := range files {
		stat, err := os.Stat(file)
		if err != nil {
			continue
		}
		if stat.IsDir() {
			continue
		}
		filesize := stat.Size()

		if defaultTruncateThreshold < filesize {
			truncate(file, filesize, lim)
		}
		if err := os.Remove(file); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func truncate(path string, size int64, lim *rate.Limiter) {
	truncateCount := (size - 1) / defaultTruncateThreshold
	for i := int64(0); i < truncateCount; i += 1 {
		nextSize := defaultTruncateThreshold * (truncateCount - i)
		os.Truncate(path, nextSize)

		r := lim.ReserveN(time.Now(), int(nextSize))
		if r.OK() != true {
			continue
		}
		if d := r.Delay(); 0 < d {
			time.Sleep(d)
		}
	}
}
