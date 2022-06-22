package bitcaskdb

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/abcum/lcp"
	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
	"github.com/octu0/bitcaskdb/repli"
)

const (
	lockfile       string = "lock"
	filerIndexFile string = "index"
	ttlIndexFile   string = "ttl_index"
	metadataFile   string = "meta.json"
)

// Stats is a struct returned by Stats() on an open Bitcask instance
type Stats struct {
	Datafiles int
	Keys      int
	Size      int64
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
	datafiles  map[int32]datafile.Datafile
	trie       art.Tree
	indexer    indexer.Indexer
	ttlIndexer indexer.Indexer
	ttlIndex   art.Tree
	metadata   *metadata
	repliEmit  repli.Emitter
	repliRecv  repli.Reciver
	isMerging  bool
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
	b.mu.RUnlock()

	return Stats{
		Datafiles: datafiles,
		Keys:      keys,
		Size:      dirSize,
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

	if err := b.close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (b *Bitcask) close() error {
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

	return b.get(key)
}

// Has returns true if the key exists in the database, false otherwise.
func (b *Bitcask) Has(key []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
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

	index, size, err := b.put(key, value, expiry)
	if err != nil {
		return err
	}

	if b.opt.Sync {
		if err := b.curr.Sync(); err != nil {
			return err
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

// Delete deletes the named key.
func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.delete(key)
}

// delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) delete(key []byte) error {
	_, _, err := b.put(key, nil, time.Time{})
	if err != nil {
		return err
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
func (b *Bitcask) Sift(f func(key []byte) (bool, error)) (err error) {
	keysToDelete := art.New()

	b.mu.RLock()
	b.trie.ForEach(func(node art.Node) bool {
		nodeKey := node.Key()
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
		b.delete(node.Key())
		return true
	})
	return
}

// DeleteAll deletes all the keys. If an I/O error occurs the error is returned.
func (b *Bitcask) DeleteAll() (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEach(func(node art.Node) bool {
		nodeKey := node.Key()
		_, _, err = b.put(nodeKey, nil, time.Time{})
		if err != nil {
			return false
		}
		filer, _ := b.trie.Search(nodeKey)
		b.metadata.ReclaimableSpace += filer.(indexer.Filer).Size
		return true
	})
	b.trie = art.New()
	b.ttlIndex = art.New()

	return
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error is returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) (err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		nodeKey := node.Key()
		// Skip the root node
		if len(nodeKey) == 0 {
			return true
		}

		if err = f(nodeKey); err != nil {
			return false
		}
		return true
	})
	return
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
		b.delete(node.Key())
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
		b.delete(node.Key())
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

// get retrieves the value of the given key
func (b *Bitcask) get(key []byte) (*datafile.Entry, error) {
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
		return nil, err
	}

	if b.opt.ValidateChecksum {
		if err := e.Validate(b.opt.RuntimeContext); err != nil {
			return nil, err
		}
	}

	return e, nil
}

func (b *Bitcask) maybeRotate() error {
	size := b.curr.Size()
	if size < int64(b.opt.MaxDatafileSize) {
		return nil
	}

	if err := b.curr.Close(); err != nil {
		return err
	}

	id := b.curr.FileID()

	df, err := datafile.OpenReadonly(
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.Path(b.path),
		datafile.FileID(id),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
		datafile.ValueOnMemoryThreshold(b.opt.ValueOnMemoryThreshold),
	)
	if err != nil {
		return err
	}

	b.datafiles[id] = df

	curr, err := datafile.Open(
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.Path(b.path),
		datafile.FileID(b.curr.FileID()+1),
		datafile.FileMode(b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
		datafile.ValueOnMemoryThreshold(b.opt.ValueOnMemoryThreshold),
	)
	if err != nil {
		return err
	}

	b.curr = curr
	if err := b.saveIndexes(); err != nil {
		return err
	}

	return nil
}

// put inserts a new (key, value). Both key and value are valid inputs.
func (b *Bitcask) put(key []byte, value io.Reader, expiry time.Time) (int64, int64, error) {
	if err := b.maybeRotate(); err != nil {
		return -1, 0, errors.Wrap(err, "error rotating active datafile")
	}

	return b.curr.Write(key, value, expiry)
}

// closeCurrentFile closes current datafile and makes it read only.
func (b *Bitcask) closeCurrentFile() error {
	if err := b.curr.Close(); err != nil {
		return err
	}

	id := b.curr.FileID()
	df, err := datafile.OpenReadonly(
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.Path(b.path),
		datafile.FileID(id),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
		datafile.ValueOnMemoryThreshold(b.opt.ValueOnMemoryThreshold),
	)
	if err != nil {
		return err
	}

	b.datafiles[id] = df
	return nil
}

// openNewWritableFile opens new datafile for writing data
func (b *Bitcask) openNewWritableFile() error {
	id := b.curr.FileID() + 1
	curr, err := datafile.Open(
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.Path(b.path),
		datafile.FileID(id),
		datafile.FileMode(b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
		datafile.ValueOnMemoryThreshold(b.opt.ValueOnMemoryThreshold),
	)
	if err != nil {
		return err
	}
	b.curr = curr
	return nil
}

// Reopen closes and reopsns the database
func (b *Bitcask) Reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.reopen()
}

// reopen reloads a bitcask object with index and datafiles
// caller of this method should take care of locking
func (b *Bitcask) reopen() error {
	datafiles, lastID, err := loadDatafiles(b.opt, b.path)
	if err != nil {
		return err
	}
	t, ttlIndex, err := loadIndexes(b, datafiles, lastID)
	if err != nil {
		return err
	}

	curr, err := datafile.Open(
		datafile.RuntimeContext(b.opt.RuntimeContext),
		datafile.Path(b.path),
		datafile.FileID(lastID),
		datafile.FileMode(b.opt.FileFileModeBeforeUmask),
		datafile.TempDir(b.opt.TempDir),
		datafile.CopyTempThreshold(b.opt.CopyTempThreshold),
		datafile.ValueOnMemoryThreshold(b.opt.ValueOnMemoryThreshold),
	)
	if err != nil {
		return err
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
	b.mu.Lock()
	if b.isMerging {
		b.mu.Unlock()
		return ErrMergeInProgress
	}
	b.isMerging = true
	b.mu.Unlock()

	defer func() {
		b.isMerging = false
	}()

	b.mu.Lock()
	if err := b.closeCurrentFile(); err != nil {
		b.mu.Unlock()
		return err
	}
	filesToMerge := make([]int32, 0, len(b.datafiles))
	for id, _ := range b.datafiles {
		filesToMerge = append(filesToMerge, id)
	}
	if err := b.openNewWritableFile(); err != nil {
		b.mu.Unlock()
		return err
	}
	b.mu.Unlock()

	sort.Slice(filesToMerge, func(i, j int) bool {
		return filesToMerge[i] < filesToMerge[j]
	})

	// Temporary merged database path
	temp, err := os.MkdirTemp(b.path, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)

	// Create a merged database
	mdb, err := Open(temp, withMerge(b.opt))
	if err != nil {
		return err
	}
	// Rewrite all key/value pairs into merged database
	// Doing this automatically strips deleted keys and
	// old key/value pairs
	if err := b.Fold(func(key []byte) error {
		filer, _ := b.trie.Search(key)
		// if key was updated after start of merge operation, nothing to do
		if filesToMerge[len(filesToMerge)-1] < filer.(indexer.Filer).FileID {
			return nil
		}
		e, err := b.get(key)
		if err != nil {
			return err
		}
		defer e.Close()

		if e.Expiry.IsZero() != true {
			if err := mdb.PutWithTTL(key, e.Value, time.Until(e.Expiry)); err != nil {
				return err
			}
		} else {
			if err := mdb.Put(key, e.Value); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}
	if err := mdb.Sync(); err != nil {
		return err
	}
	if err := mdb.Close(); err != nil {
		return err
	}

	// no reads and writes till we reopen
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.close(); err != nil {
		return err
	}

	// Remove data files
	files, err := os.ReadDir(b.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() || file.Name() == lockfile {
			continue
		}
		ids, err := datafile.ParseIds([]string{file.Name()})
		if err != nil {
			return err
		}
		// if datafile was created after start of merge, skip
		if 0 < len(ids) && filesToMerge[len(filesToMerge)-1] < ids[0] {
			continue
		}
		if err := os.RemoveAll(path.Join(b.path, file.Name())); err != nil {
			return err
		}
	}

	// Rename all merged data files
	files, err = os.ReadDir(mdb.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		// see #225
		if file.Name() == lockfile {
			continue
		}
		if err := os.Rename(
			path.Join([]string{mdb.path, file.Name()}...),
			path.Join([]string{b.path, file.Name()}...),
		); err != nil {
			return err
		}
	}
	b.metadata.ReclaimableSpace = 0

	// And finally reopen the database
	return b.reopen()
}

// saveIndex saves index and ttl_index currently in RAM to disk
func (b *Bitcask) saveIndexes() error {
	tempIdx := "temp_index"
	if err := b.indexer.Save(b.trie, filepath.Join(b.path, tempIdx)); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(b.path, tempIdx), filepath.Join(b.path, filerIndexFile)); err != nil {
		return err
	}
	if err := b.ttlIndexer.Save(b.ttlIndex, filepath.Join(b.path, tempIdx)); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(b.path, tempIdx), filepath.Join(b.path, ttlIndexFile)); err != nil {
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

// Reclaimable returns space that can be reclaimed
func (b *Bitcask) Reclaimable() int64 {
	return b.metadata.ReclaimableSpace
}

// isExpired returns true if a key has expired
// it returns false if key does not exist in ttl index
func (b *Bitcask) isExpired(key []byte) bool {
	expiry, found := b.ttlIndex.Search(key)
	if found != true {
		return false
	}
	return expiry.(time.Time).Before(time.Now().UTC())
}

func (b *Bitcask) repliSource() repli.Source {
	return newRepliSource(b)
}

func (b *Bitcask) repliDestination() repli.Destination {
	return newRepliDestination(b)
}

func loadDatafiles(opt *option, path string) (map[int32]datafile.Datafile, int32, error) {
	ids, err := datafile.ParseIdsFromDatafiles(path)
	if err != nil {
		return nil, 0, err
	}

	datafiles := make(map[int32]datafile.Datafile, len(ids))
	for _, id := range ids {
		d, err := datafile.OpenReadonly(
			datafile.RuntimeContext(opt.RuntimeContext),
			datafile.Path(path),
			datafile.FileID(id),
			datafile.TempDir(opt.TempDir),
			datafile.CopyTempThreshold(opt.CopyTempThreshold),
			datafile.ValueOnMemoryThreshold(opt.ValueOnMemoryThreshold),
		)
		if err != nil {
			return nil, 0, err
		}
		datafiles[id] = d
	}
	if len(ids) < 1 {
		return datafiles, 0, nil
	}

	lastID := ids[len(ids)-1]
	return datafiles, lastID, nil
}

func getSortedDatafiles(datafiles map[int32]datafile.Datafile) []datafile.Datafile {
	out := make([]datafile.Datafile, 0, len(datafiles))
	for _, df := range datafiles {
		out = append(out, df)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FileID() < out[j].FileID()
	})
	return out
}

// loadIndexes loads index from disk to memory. If index is not available or partially available (last bitcask process crashed)
// then it iterates over last datafile and construct index
// we construct ttl_index here also along with normal index
func loadIndexes(b *Bitcask, datafiles map[int32]datafile.Datafile, lastID int32) (art.Tree, art.Tree, error) {
	t, found, err := b.indexer.Load(filepath.Join(b.path, filerIndexFile))
	if err != nil {
		return nil, nil, err
	}
	ttlIndex, _, err := b.ttlIndexer.Load(filepath.Join(b.path, ttlIndexFile))
	if err != nil {
		return nil, nil, err
	}
	if found && b.metadata.IndexUpToDate {
		return t, ttlIndex, nil
	}
	if found {
		if err := loadIndexFromDatafile(t, ttlIndex, datafiles[lastID]); err != nil {
			return nil, ttlIndex, err
		}
		return t, ttlIndex, nil
	}

	sortedDatafiles := getSortedDatafiles(datafiles)
	for _, df := range sortedDatafiles {
		if err := loadIndexFromDatafile(t, ttlIndex, df); err != nil {
			return nil, ttlIndex, err
		}
	}
	return t, ttlIndex, nil
}

func loadIndexFromDatafile(t art.Tree, ttlIndex art.Tree, df datafile.Datafile) error {
	index := int64(0)
	for {
		e, err := df.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if e.ValueSize == 0 {
			// Tombstone value  (deleted key)
			t.Delete(e.Key)
			index += e.TotalSize
			continue
		}

		t.Insert(e.Key, indexer.Filer{
			FileID: df.FileID(),
			Index:  index,
			Size:   e.TotalSize,
		})
		if e.Expiry.IsZero() != true {
			ttlIndex.Insert(e.Key, e.Expiry)
		}
		index += e.TotalSize
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
			return err
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
			return nil, err
		}
	}

	if err := os.MkdirAll(path, opt.DirFileModeBeforeUmask); err != nil {
		return nil, err
	}

	meta, err := loadMetadata(path)
	if err != nil {
		return nil, &ErrBadMetadata{err}
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
	}

	if err := repliEmitter.Start(bitcask.repliSource(), opt.RepliBindIP, opt.RepliBindPort); err != nil {
		return nil, errors.WithStack(err)
	}

	ok, err := bitcask.flock.TryLock()
	if err != nil {
		return nil, err
	}
	if ok != true {
		return nil, ErrDatabaseLocked
	}

	if err := bitcask.Reopen(); err != nil {
		return nil, err
	}

	if err := repliReciver.Start(bitcask.repliDestination(), opt.RepliServerIP, opt.RepliServerPort); err != nil {
		return nil, errors.WithStack(err)
	}
	return bitcask, nil
}
