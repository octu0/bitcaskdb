package bitcaskdb

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/abcum/lcp"
	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"
	"golang.org/x/time/rate"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
	"github.com/octu0/bitcaskdb/repli"
)

const (
	lockfile       string = "lock"
	filerIndexFile string = "index"
	ttlIndexFile   string = "ttl_index"
	metadataFile   string = "meta.json"
	tempIndexFile  string = "index.tmp"
)

// Stats is a struct returned by Stats() on an open Bitcask instance
type Stats struct {
	Datafiles        int
	Keys             int
	Size             int64
	ReclaimableSpace int64
}

type metadata struct {
	IndexUpToDate    bool  `json:"index_up_to_date"`
	ReclaimableSpace int64 `json:"reclaimable_space"`
}

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu         *sync.RWMutex
	flock      *flock.Flock
	opt        *option
	path       string
	curr       datafile.Datafile
	datafiles  map[datafile.FileID]datafile.Datafile
	trie       art.Tree
	indexer    indexer.Indexer
	ttlIndexer indexer.Indexer
	ttlIndex   art.Tree
	metadata   *metadata
	repliEmit  repli.Emitter
	repliRecv  repli.Reciver
	merger     *merger
}

// Stats returns statistics about the database including the number of
// data files, keys and overall size on disk of the data
func (b *Bitcask) Stats() (Stats, error) {
	dirSize, err := calcDirSize(b.path)
	if err != nil {
		return Stats{}, errors.WithStack(err)
	}

	b.mu.RLock()
	datafiles := len(b.datafiles)
	keys := b.trie.Size()
	rs := b.metadata.ReclaimableSpace
	b.mu.RUnlock()

	return Stats{
		Datafiles:        datafiles,
		Keys:             keys,
		Size:             dirSize,
		ReclaimableSpace: rs,
	}, nil
}

// Close closes the database and removes the lock. It is important to call
// Close() as this is the only way to cleanup the lock held by the open
// database.
func (b *Bitcask) Close() error {
	b.mu.RLock()
	defer func() {
		b.mu.RUnlock()
		b.flock.Unlock()
	}()

	if err := b.repliEmit.Stop(); err != nil {
		return errors.WithStack(err)
	}
	if err := b.repliRecv.Stop(); err != nil {
		return errors.WithStack(err)
	}

	if err := b.closeLocked(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bitcask) closeLocked() error {
	if err := b.saveIndexes(); err != nil {
		return errors.WithStack(err)
	}

	b.metadata.IndexUpToDate = true
	if err := b.saveMetadata(); err != nil {
		return errors.WithStack(err)
	}

	for _, df := range b.datafiles {
		if err := df.Close(); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := b.curr.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Sync flushes all buffers to disk ensuring all data is written
func (b *Bitcask) Sync() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.saveMetadata(); err != nil {
		return errors.WithStack(err)
	}

	if err := b.curr.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Get fetches value for a key
func (b *Bitcask) Get(key []byte) (io.ReadCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.getLocked(key)
}

// get retrieves the value of the given key
func (b *Bitcask) getLocked(key []byte) (*datafile.Entry, error) {
	value, found := b.trie.Search(key)
	if found != true {
		return nil, ErrKeyNotFound
	}
	if b.isExpired(key) {
		return nil, ErrKeyExpired
	}

	filer := value.(indexer.Filer)

	var df datafile.Datafile
	if filer.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[filer.FileID]
	}

	e, err := df.ReadAt(filer.Index, filer.Size)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if b.opt.ValidateChecksum {
		if err := e.Validate(b.opt.RuntimeContext); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return e, nil
}

// Has returns true if the key exists in the database, false otherwise.
func (b *Bitcask) Has(key []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.hasLocked(key)
}

func (b *Bitcask) hasLocked(key []byte) bool {
	_, found := b.trie.Search(key)
	if found != true {
		return false
	}
	if b.isExpired(key) {
		return false
	}
	return true
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key []byte, value io.Reader) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	return b.putAndIndex(key, value, time.Time{})
}

func (b *Bitcask) PutBytes(key, value []byte) error {
	if value == nil {
		return b.Put(key, nil)
	}
	return b.Put(key, bytes.NewReader(value))
}

// PutWithTTL stores the key and value in the database with the given TTL
func (b *Bitcask) PutWithTTL(key []byte, value io.Reader, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	return b.putAndIndex(key, value, time.Now().Add(ttl))
}

func (b *Bitcask) PutBytesWithTTL(key, value []byte, ttl time.Duration) error {
	if value == nil {
		return b.PutWithTTL(key, nil, ttl)
	}
	return b.PutWithTTL(key, bytes.NewReader(value), ttl)
}

func (b *Bitcask) putAndIndex(key []byte, value io.Reader, expiry time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.putAndIndexLocked(key, value, expiry)
}

func (b *Bitcask) putAndIndexLocked(key []byte, value io.Reader, expiry time.Time) error {
	index, size, err := b.put(key, value, expiry)
	if err != nil {
		return errors.WithStack(err)
	}

	if b.opt.Sync {
		if err := b.curr.Sync(); err != nil {
			return errors.WithStack(err)
		}
	}

	// in case of successful `put`, IndexUpToDate will be always be false
	b.metadata.IndexUpToDate = false

	if oldFiler, found := b.trie.Search(key); found {
		b.metadata.ReclaimableSpace += oldFiler.(indexer.Filer).Size
	}

	f := indexer.Filer{
		FileID: b.curr.FileID(),
		Index:  index,
		Size:   size,
	}
	b.trie.Insert(key, f)
	if expiry.IsZero() != true {
		b.ttlIndex.Insert(key, expiry)
	}
	b.repliEmit.EmitInsert(f)
	return nil
}

// put inserts a new (key, value). Both key and value are valid inputs.
func (b *Bitcask) put(key []byte, value io.Reader, expiry time.Time) (int64, int64, error) {
	if err := b.maybeRotate(); err != nil {
		return -1, 0, errors.Wrap(err, "error rotating active datafile")
	}

	return b.curr.Write(key, value, expiry)
}

// Delete deletes the named key.
func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.deleteLocked(key)
}

// delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) deleteLocked(key []byte) error {
	_, _, err := b.put(key, nil, time.Time{})
	if err != nil {
		return errors.WithStack(err)
	}

	v, found := b.trie.Search(key)
	if found {
		f := v.(indexer.Filer)
		b.metadata.ReclaimableSpace += f.Size
	}

	b.trie.Delete(key)
	b.ttlIndex.Delete(key)

	// remove current
	b.repliEmit.EmitDelete(key)

	return nil
}

// Sift iterates over all keys in the database calling the function `f` for
// each key. If the KV pair is expired or the function returns true, that key is
// deleted from the database.
// If the function returns an error on any key, no further keys are processed, no
// keys are deleted, and the first error is returned.
func (b *Bitcask) Sift(f func(key []byte) (bool, error)) error {
	keysToDelete := art.New()

	var lastErr error
	b.mu.RLock()
	b.trie.ForEach(func(node art.Node) bool {
		nodeKey := node.Key()
		if b.isExpired(nodeKey) {
			keysToDelete.Insert(nodeKey, true)
			return true
		}
		var shouldDelete bool
		shouldDelete, err := f(nodeKey)
		if err != nil {
			lastErr = errors.WithStack(err)
			return false
		}
		if shouldDelete {
			keysToDelete.Insert(nodeKey, true)
		}
		return true
	})
	b.mu.RUnlock()
	if lastErr != nil {
		return errors.WithStack(lastErr)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.deleteLocked(node.Key())
		return true
	})
	return nil
}

// DeleteAll deletes all the keys. If an I/O error occurs the error is returned.
func (b *Bitcask) DeleteAll() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var lastErr error
	b.trie.ForEach(func(node art.Node) bool {
		nodeKey := node.Key()
		if _, _, err := b.put(nodeKey, nil, time.Time{}); err != nil {
			lastErr = errors.WithStack(err)
			return false
		}

		filer, _ := b.trie.Search(nodeKey)
		b.metadata.ReclaimableSpace += filer.(indexer.Filer).Size
		return true
	})
	b.trie = art.New()
	b.ttlIndex = art.New()

	if lastErr != nil {
		return errors.WithStack(lastErr)
	}
	return nil
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error is returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var lastErr error
	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		nodeKey := node.Key()
		// Skip the root node
		if len(nodeKey) == 0 {
			return true
		}

		// skip expired
		if b.isExpired(nodeKey) {
			return true
		}

		if err := f(nodeKey); err != nil {
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

// SiftScan iterates over all keys in the database beginning with the given
// prefix, calling the function `f` for each key. If the KV pair is expired or
// the function returns true, that key is deleted from the database.
//  If the function returns an error on any key, no further keys are processed,
// no keys are deleted, and the first error is returned.
func (b *Bitcask) SiftScan(prefix []byte, f func(key []byte) (bool, error)) (err error) {
	keysToDelete := art.New()

	b.mu.RLock()
	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		nodeKey := node.Key()
		// Skip the root node
		if len(nodeKey) == 0 {
			return true
		}
		if b.isExpired(nodeKey) {
			keysToDelete.Insert(nodeKey, true)
			return true
		}
		var shouldDelete bool
		if shouldDelete, err = f(nodeKey); err != nil {
			return false
		} else if shouldDelete {
			keysToDelete.Insert(nodeKey, true)
		}
		return true
	})
	b.mu.RUnlock()

	if err != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.deleteLocked(node.Key())
		return true
	})
	return
}

// Range performs a range scan of keys matching a range of keys between the
// start key and end key and calling the function `f` with the keys found.
// If the function returns an error no further keys are processed and the
// first error returned.
func (b *Bitcask) Range(start, end []byte, f func(key []byte) error) (err error) {
	if bytes.Compare(start, end) == 1 {
		return ErrInvalidRange
	}

	commonPrefix := lcp.LCP(start, end)
	if commonPrefix == nil {
		return ErrInvalidRange
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEachPrefix(commonPrefix, func(node art.Node) bool {
		nodeKey := node.Key()
		if bytes.Compare(nodeKey, start) >= 0 && bytes.Compare(nodeKey, end) <= 0 {
			if err = f(node.Key()); err != nil {
				return false
			}
			return true
		}
		if bytes.Compare(nodeKey, start) >= 0 && bytes.Compare(nodeKey, end) > 0 {
			return false
		}
		return true
	})
	return
}

// SiftRange performs a range scan of keys matching a range of keys between the
// start key and end key and calling the function `f` with the keys found.
// If the KV pair is expired or the function returns true, that key is deleted
// from the database.
// If the function returns an error on any key, no further keys are processed, no
// keys are deleted, and the first error is returned.
func (b *Bitcask) SiftRange(start, end []byte, f func(key []byte) (bool, error)) (err error) {
	if bytes.Compare(start, end) == 1 {
		return ErrInvalidRange
	}

	commonPrefix := lcp.LCP(start, end)
	if commonPrefix == nil {
		return ErrInvalidRange
	}

	keysToDelete := art.New()

	b.mu.RLock()
	b.trie.ForEachPrefix(commonPrefix, func(node art.Node) bool {
		nodeKey := node.Key()
		if bytes.Compare(nodeKey, start) >= 0 && bytes.Compare(nodeKey, end) <= 0 {
			if b.isExpired(nodeKey) {
				keysToDelete.Insert(nodeKey, true)
				return true
			}
			var shouldDelete bool
			if shouldDelete, err = f(nodeKey); err != nil {
				return false
			} else if shouldDelete {
				keysToDelete.Insert(nodeKey, true)
			}
			return true
		}
		if bytes.Compare(nodeKey, start) >= 0 && bytes.Compare(nodeKey, end) > 0 {
			return false
		}
		return true
	})
	b.mu.RUnlock()

	if err != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.deleteLocked(node.Key())
		return true
	})

	return
}

// Len returns the total number of keys in the database
func (b *Bitcask) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.trie.Size()
}

// Keys returns all keys in the database as a channel of keys
func (b *Bitcask) Keys() chan []byte {
	ch := make(chan []byte)
	go func() {
		b.mu.RLock()
		defer b.mu.RUnlock()

		for it := b.trie.Iterator(); it.HasNext(); {
			node, _ := it.Next()
			if b.isExpired(node.Key()) {
				continue
			}
			ch <- node.Key()
		}
		close(ch)
	}()

	return ch
}

// RunGC deletes all expired keys
func (b *Bitcask) RunGC() error {
	keysToDelete := art.New()
	b.mu.RLock()
	b.ttlIndex.ForEach(func(node art.Node) (cont bool) {
		if b.isExpired(node.Key()) != true {
			// later, return false here when the ttlIndex is sorted
			return true
		}
		keysToDelete.Insert(node.Key(), true)
		return true
	})
	b.mu.RUnlock()

	keysToDelete.ForEach(func(node art.Node) (cont bool) {
		b.Delete(node.Key())
		return true
	})

	return nil
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error is returned.
func (b *Bitcask) Fold(f func(key []byte) error) (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		if err = f(node.Key()); err != nil {
			return false
		}
		return true
	})

	return
}

func (b *Bitcask) maybeRotate() error {
	size := b.curr.Size()
	if size < int64(b.opt.MaxDatafileSize) {
		return nil
	}

	if _, err := b.closeCurrentFile(); err != nil {
		return errors.WithStack(err)
	}

	if err := b.openWritableFile(datafile.NextFileID()); err != nil {
		return errors.WithStack(err)
	}

	if err := b.saveIndexes(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// closeCurrentFile closes current datafile and makes it read only.
func (b *Bitcask) closeCurrentFile() (datafile.FileID, error) {
	id := b.curr.FileID()

	if err := b.curr.Close(); err != nil {
		return datafile.FileID{}, errors.WithStack(err)
	}

	df, err := datafile.OpenReadonly(id, b.path,
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
	)
	if err != nil {
		if err2 := b.openWritableFile(id); err2 != nil {
			return datafile.FileID{}, errors.Wrapf(err2, "failed reopen datafile(writable) %s(%d) cause:%+v", b.path, id, err)
		}
		return datafile.FileID{}, errors.Wrapf(err, "failed to open datafile(readonly) %s(%d)", b.path, id)
	}

	b.datafiles[id] = df
	return id, nil
}

// openWritableFile opens new datafile for writing data
func (b *Bitcask) openWritableFile(fileID datafile.FileID) error {
	curr, err := datafile.Open(fileID, b.path,
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.FileMode(b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	b.curr = curr
	b.repliEmit.EmitCurrentFileID(fileID)
	return nil
}

// Reopen closes and reopsns the database
func (b *Bitcask) Reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.reopenLocked()
}

// reopen reloads a bitcask object with index and datafiles
// caller of this method should take care of locking
func (b *Bitcask) reopenLocked() error {
	datafiles, lastFileID, err := loadDatafiles(b.opt, b.path)
	if err != nil {
		return errors.WithStack(err)
	}
	t, ttlIndex, err := loadIndexes(b, datafiles, lastFileID)
	if err != nil {
		return errors.WithStack(err)
	}

	curr, err := datafile.Open(lastFileID, b.path,
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.FileMode(b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
	)
	if err != nil {
		return errors.WithStack(err)
	}

	b.trie = t
	b.curr = curr
	b.ttlIndex = ttlIndex
	b.datafiles = datafiles

	return nil
}

// Merge merges all datafiles in the database. Old keys are squashed
// and deleted keys removes. Duplicate key/value pairs are also removed.
// Call this function periodically to reclaim disk space.
func (b *Bitcask) Merge() error {
	return b.merger.Merge(b, rate.NewLimiter(rate.Inf, math.MaxInt))
}

func (b *Bitcask) MergeWithWaitLimit(lim *rate.Limiter) error {
	return b.merger.Merge(b, lim)
}

func (b *Bitcask) MergeWithWaitLimitByBytesPerSecond(bytesPerSecond int) error {
	return b.merger.Merge(b, rate.NewLimiter(rate.Limit(float64(bytesPerSecond)), bytesPerSecond))
}

// saveIndex saves index and ttl_index currently in RAM to disk
func (b *Bitcask) saveIndexes() error {
	if err := b.indexer.Save(b.trie, filepath.Join(b.path, tempIndexFile)); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(b.path, tempIndexFile), filepath.Join(b.path, filerIndexFile)); err != nil {
		return err
	}
	if err := b.ttlIndexer.Save(b.ttlIndex, filepath.Join(b.path, tempIndexFile)); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(b.path, tempIndexFile), filepath.Join(b.path, ttlIndexFile)); err != nil {
		return err
	}
	return nil
}

// saveMetadata saves metadata into disk
func (b *Bitcask) saveMetadata() error {
	if err := saveMetadata(b.path, b.metadata, b.opt.DirFileModeBeforeUmask); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// isExpired returns true if a key has expired
// it returns false if key does not exist in ttl index
func (b *Bitcask) isExpired(key []byte) bool {
	expiry, found := b.ttlIndex.Search(key)
	if found != true {
		return false
	}
	return isExpiredFromTime(expiry.(time.Time))
}

func (b *Bitcask) repliSource() repli.Source {
	return newRepliSource(b)
}

func (b *Bitcask) repliDestination() repli.Destination {
	return newRepliDestination(b)
}

func isExpiredFromTime(expiry time.Time) bool {
	if expiry.IsZero() {
		return false
	}
	return expiry.Before(time.Now().UTC())
}

func loadDatafiles(opt *option, path string) (map[datafile.FileID]datafile.Datafile, datafile.FileID, error) {
	ids, err := datafile.GrepFileIdsFromDatafilePath(path)
	if err != nil {
		return nil, datafile.FileID{}, err
	}

	datafiles := make(map[datafile.FileID]datafile.Datafile, len(ids))
	for _, id := range ids {
		d, err := datafile.OpenReadonly(id, path,
			datafile.RuntimeContext(opt.RuntimeContext),
			datafile.TempDir(opt.TempDir),
			datafile.CopyTempThreshold(opt.CopyTempThreshold),
		)
		if err != nil {
			return nil, datafile.FileID{}, errors.WithStack(err)
		}
		datafiles[id] = d
	}
	if len(ids) < 1 {
		return datafiles, datafile.FileID{}, nil
	}

	lastID := ids[len(ids)-1]
	return datafiles, lastID, nil
}

// loadIndexes loads index from disk to memory. If index is not available or partially available (last bitcask process crashed)
// then it iterates over last datafile and construct index
// we construct ttl_index here also along with normal index
func loadIndexes(b *Bitcask, datafiles map[datafile.FileID]datafile.Datafile, lastID datafile.FileID) (art.Tree, art.Tree, error) {
	t, found, err := b.indexer.Load(filepath.Join(b.path, filerIndexFile))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	ttlIndex, _, err := b.ttlIndexer.Load(filepath.Join(b.path, ttlIndexFile))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if found && b.metadata.IndexUpToDate {
		return t, ttlIndex, nil
	}
	if found {
		if err := loadIndexFromDatafile(t, ttlIndex, datafiles[lastID], nil); err != nil {
			return nil, ttlIndex, errors.WithStack(err)
		}
		return t, ttlIndex, nil
	}

	fileIds := make([]datafile.FileID, 0, len(datafiles))
	for _, df := range datafiles {
		fileIds = append(fileIds, df.FileID())
	}
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i].Newer(fileIds[j])
	})

	for _, fileID := range fileIds {
		df := datafiles[fileID]
		if err := loadIndexFromDatafile(t, ttlIndex, df, nil); err != nil {
			return nil, ttlIndex, errors.WithStack(err)
		}
	}
	return t, ttlIndex, nil
}

func loadIndexFromDatafile(t art.Tree, ttlIndex art.Tree, df datafile.Datafile, lim *rate.Limiter) error {
	if lim == nil {
		lim = rate.NewLimiter(rate.Inf, math.MaxInt)
	}

	index := int64(0)
	for {
		e, err := df.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		defer e.Close()

		if 0 < e.ValueSize {
			t.Insert(e.Key, indexer.Filer{
				FileID: df.FileID(),
				Index:  index,
				Size:   e.TotalSize,
			})
			if e.Expiry.IsZero() != true {
				ttlIndex.Insert(e.Key, e.Expiry)
			}
			index += e.TotalSize
		} else {
			// Tombstone value  (deleted key)
			t.Delete(e.Key)
			index += e.TotalSize
		}

		r := lim.ReserveN(time.Now(), int(e.TotalSize))
		if r.OK() != true {
			continue
		}
		if d := r.Delay(); 0 < d {
			time.Sleep(d)
		}
	}
	return nil
}

func saveMetadata(path string, meta *metadata, fmode os.FileMode) error {
	outPath := filepath.Join(path, metadataFile)

	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fmode)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(meta); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func loadMetadata(path string) (*metadata, error) {
	filePath := filepath.Join(path, metadataFile)

	if _, err := os.Stat(filePath); err != nil {
		// no file
		return new(metadata), nil
	}

	// exists
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer f.Close()

	meta := new(metadata)
	if err := json.NewDecoder(f).Decode(meta); err != nil {
		return nil, errors.WithStack(err)
	}
	return meta, nil
}

// calcDirSize returns the space occupied by the given `path` on disk on the current
// file system.
func calcDirSize(path string) (int64, error) {
	size := int64(0)
	if err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if info.IsDir() != true {
			size += info.Size()
		}
		return nil
	}); err != nil {
		return 0, errors.WithStack(err)
	}
	return size, nil
}

func createRepliEmitter(opt *option) repli.Emitter {
	if opt.NoRepliEmit {
		return repli.NewNoopEmitter()
	}
	return repli.NewStreamEmitter(opt.RuntimeContext, opt.Logger, opt.TempDir, int32(opt.CopyTempThreshold))
}

func createRepliReciver(opt *option) repli.Reciver {
	if opt.NoRepliRecv {
		return repli.NewNoopReciver()
	}
	return repli.NewStreamReciver(opt.RuntimeContext, opt.Logger, opt.TempDir, opt.RepliRequestTimeout)
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, funcs ...OptionFunc) (*Bitcask, error) {
	opt := newDefaultOption()
	for _, fn := range funcs {
		if err := fn(opt); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if err := os.MkdirAll(path, opt.DirFileModeBeforeUmask); err != nil {
		return nil, errors.WithStack(err)
	}

	meta, err := loadMetadata(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	repliEmitter := createRepliEmitter(opt)
	repliReciver := createRepliReciver(opt)
	bitcask := &Bitcask{
		mu:         new(sync.RWMutex),
		flock:      flock.New(filepath.Join(path, lockfile)),
		opt:        opt,
		path:       path,
		indexer:    indexer.NewFilerIndexer(opt.RuntimeContext),
		ttlIndexer: indexer.NewTTLIndexer(opt.RuntimeContext),
		metadata:   meta,
		repliEmit:  repliEmitter,
		repliRecv:  repliReciver,
		merger:     newMerger(),
	}

	if err := repliEmitter.Start(bitcask.repliSource(), opt.RepliBindIP, opt.RepliBindPort); err != nil {
		return nil, errors.WithStack(err)
	}

	ok, err := bitcask.flock.TryLock()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if ok != true {
		return nil, errors.WithStack(ErrDatabaseLocked)
	}

	if err := bitcask.Reopen(); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := repliReciver.Start(bitcask.repliDestination(), opt.RepliServerIP, opt.RepliServerPort); err != nil {
		return nil, errors.WithStack(err)
	}
	return bitcask, nil
}
