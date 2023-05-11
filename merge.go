package bitcaskdb

import (
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/octu0/priorate"
	"github.com/pkg/errors"
	"github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
)

const (
	mergeDirPattern  string = "merge*"
	removeParkPrefix string = ".mark.%s"
	snapshotTrieFile string = "snapshot_trie"
)

const (
	defaultTruncateThreshold int64         = 100 * 1024 * 1024
	defaultSlowTruncateWait  time.Duration = 100 * time.Millisecond
)

type merger struct {
	mutex   *sync.RWMutex
	opt     *option
	basedir string
	merging bool
}

func (m *merger) isMerging() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.merging
}

func (m *merger) Merge(b *Bitcask, lim *priorate.Limiter) error {
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
	start := time.Now()

	m.opt.Logger.Printf("info: merge / rotate datafiles (%s)", time.Since(start))
	lastFileID, mergeFileIds, err := m.rotateCurrentDafafile(b)
	if err != nil {
		return errors.WithStack(err)
	}

	m.opt.Logger.Printf("info: merge / snapshot indexer (%s)", time.Since(start))
	snapshot, err := m.snapshotIndexer(b, lim)
	if err != nil {
		return errors.WithStack(err)
	}
	defer snapshot.Destroy(lim)

	m.opt.Logger.Printf("info: merge / prepare merge db (%s)", time.Since(start))
	temp, mergedFiler, err := m.renewMergedDB(b, mergeFileIds, snapshot, lim)
	if err != nil {
		return errors.WithStack(err)
	}
	defer temp.Destroy(lim)

	m.opt.Logger.Printf("info: merge / concatenate merge db and current db (%s)", time.Since(start))
	// Reduce b blocking time by performing b.mu.Lock/Unlock within merger.reopen()
	removeMarkedFiles, err := m.reopen(b, temp, lastFileID, lim)
	if err != nil {
		return errors.WithStack(err)
	}

	b.repliEmit.EmitMerge(mergedFiler)

	m.opt.Logger.Printf("info: merge / cleanup files marked for remove (%s)", time.Since(start))
	removeFileSlowly(removeMarkedFiles, lim)

	m.opt.Logger.Printf("info: merge complete (%s)", time.Since(start))
	return nil
}

func (m *merger) tellSaveIndexCostLocked(b *Bitcask, lim *priorate.Limiter) {
	saveCostFiler := b.trie.Size() * indexer.FilerByteSize
	saveCostTTL := b.ttlIndex.Size() * 8

	lim.ReserveN(priorate.High, time.Now(), saveCostFiler)
	lim.ReserveN(priorate.High, time.Now(), saveCostTTL)
}

func (m *merger) tellLoadIndexCostLocked(b *Bitcask, lim *priorate.Limiter) {
	loadCostFiler := b.trie.Size() * indexer.FilerByteSize
	loadCostTTL := b.ttlIndex.Size() * 8

	lim.ReserveN(priorate.Low, time.Now(), loadCostFiler)
	lim.ReserveN(priorate.Low, time.Now(), loadCostTTL)
}

func (m *merger) reopen(b *Bitcask, temp *mergeTempDB, lastFileID datafile.FileID, lim *priorate.Limiter) ([]string, error) {
	// no reads and writes till reopen
	b.mu.Lock()
	defer b.mu.Unlock()

	m.tellSaveIndexCostLocked(b, lim)
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

	m.tellLoadIndexCostLocked(b, lim)
	// And finally reopen the database
	if err := b.reopenLocked(); err != nil {
		removeFileSlowly(removeMarkedFiles, nil)
		return nil, errors.WithStack(err)
	}
	return removeMarkedFiles, nil
}

func (m *merger) rotateCurrentDafafile(b *Bitcask) (datafile.FileID, []datafile.FileID, error) {
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

func (m *merger) snapshotIndexer(b *Bitcask, lim *priorate.Limiter) (*snapshotTrie, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	st, err := openSnapshotTrie(m.opt.TempDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed open snapshot trie")
	}

	var lastErr error
	b.trie.ForEach(func(node art.Node) bool {
		if r := lim.ReserveN(priorate.Low, time.Now(), indexer.FilerByteSize); r.OK() {
			if d := r.Delay(); 0 < d {
				time.Sleep(d)
			}
		}

		if err := st.Write(node.Key(), node.Value().(indexer.Filer)); err != nil {
			lastErr = errors.WithStack(err)
			return false
		}
		return true
	})
	if lastErr != nil {
		return nil, errors.WithStack(lastErr)
	}
	return st, nil
}

func (m *merger) renewMergedDB(b *Bitcask, mergeFileIds []datafile.FileID, st *snapshotTrie, lim *priorate.Limiter) (*mergeTempDB, []indexer.MergeFiler, error) {
	temp, err := openMergeTempDB(m.opt, m.basedir)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	mergedFiler, err := temp.MergeDatafiles(b, mergeFileIds, st, lim)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if err := temp.SyncAndClose(); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return temp, mergedFiler, nil
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

func newMerger(opt *option, basedir string) *merger {
	return &merger{
		mutex:   new(sync.RWMutex),
		opt:     opt,
		basedir: basedir,
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
func (t *mergeTempDB) MergeDatafiles(src *Bitcask, mergeFileIds []datafile.FileID, st *snapshotTrie, lim *priorate.Limiter) ([]indexer.MergeFiler, error) {
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
			return nil, errors.WithStack(err)
		}
		m[fileID] = df
	}

	mergedFiler, err := t.mergeDatafileLocked(st, m, lim)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return mergedFiler, nil
}

func (t *mergeTempDB) mergeDatafileLocked(st *snapshotTrie, m map[datafile.FileID]datafile.Datafile, lim *priorate.Limiter) ([]indexer.MergeFiler, error) {
	mergedFiler := make([]indexer.MergeFiler, 0, len(m))
	if err := st.ReadAll(func(data snapshotTrieData) error {
		filer := data.Value

		df, ok := m[filer.FileID]
		if ok != true {
			return nil
		}

		if rr := lim.ReserveN(priorate.Low, time.Now(), int(filer.Size)); rr.OK() {
			if d := rr.Delay(); 0 < d {
				time.Sleep(d)
			}
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

		if rw := lim.ReserveN(priorate.Low, time.Now(), int(e.TotalSize)); rw.OK() {
			if d := rw.Delay(); 0 < d {
				time.Sleep(d)
			}
		}

		if _, _, err := t.mdb.put(e.Key, e.Value, e.Expiry); err != nil {
			return errors.WithStack(err)
		}
		mergedFiler = append(mergedFiler, indexer.MergeFiler{
			FileID: t.mdb.curr.FileID(),
			Filer:  filer,
		})

		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}
	return mergedFiler, nil
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

func (t *mergeTempDB) Destroy(lim *priorate.Limiter) {
	if t.destroyed {
		return
	}

	runtime.SetFinalizer(t, nil) // clear
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

func openMergeTempDB(opt *option, basedir string) (*mergeTempDB, error) {
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
	runtime.SetFinalizer(st, finalizeMergeTempDB)
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

func (st *snapshotTrie) Destroy(lim *priorate.Limiter) {
	if st.destroyed {
		return
	}

	runtime.SetFinalizer(st, nil)
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
	runtime.SetFinalizer(st, finalizeSnapshotTrie)
	return st, nil
}

func removeFileSlowly(files []string, lim *priorate.Limiter) error {
	if lim == nil {
		lim = priorate.InfLimiter()
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

		if lim.Limit() < math.MaxInt && int64(lim.Limit()) < filesize {
			truncate(file, filesize, lim)
		}
		if err := os.Remove(file); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func truncate(path string, size int64, lim *priorate.Limiter) {
	threshold := int64(defaultTruncateThreshold)
	if lim.Limit() < math.MaxInt {
		threshold = int64(lim.Limit())
	}

	truncateCount := (size - 1) / threshold
	for i := int64(0); i < truncateCount; i += 1 {
		nextSize := threshold * (truncateCount - i)

		if r := lim.ReserveN(priorate.Low, time.Now(), int(nextSize)); r.OK() {
			if d := r.Delay(); 0 < d {
				time.Sleep(d + defaultSlowTruncateWait)
			}
		}

		os.Truncate(path, nextSize)
	}
}
