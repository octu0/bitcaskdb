package bitcask

import (
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"github.com/gofrs/flock"
	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
	"github.com/prologic/bitcask/internal/config"
	"github.com/prologic/bitcask/internal/data"
	"github.com/prologic/bitcask/internal/index"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyTooLarge is the error returned for a key that exceeds the
	// maximum allowed key size (configured with WithMaxKeySize).
	ErrKeyTooLarge = errors.New("error: key too large")

	// ErrValueTooLarge is the error returned for a value that exceeds the
	// maximum allowed value size (configured with WithMaxValueSize).
	ErrValueTooLarge = errors.New("error: value too large")

	// ErrChecksumFailed is the error returned if a key/value retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")

	// ErrDatabaseLocked is the error returned if the database is locked
	// (typically opened by another process)
	ErrDatabaseLocked = errors.New("error: database locked")
)

// Bitcask is a struct that represents a on-disk LSM and WAL data structure
// and in-memory hash of key/value pairs as per the Bitcask paper and seen
// in the Riak database.
type Bitcask struct {
	mu sync.RWMutex

	*flock.Flock

	config    *config.Config
	options   []Option
	path      string
	curr      data.Datafile
	datafiles map[int]data.Datafile
	trie      art.Tree
	indexer   index.Indexer
}

// Stats is a struct returned by Stats() on an open Bitcask instance
type Stats struct {
	Datafiles int
	Keys      int
	Size      int64
}

// Stats returns statistics about the database including the number of
// data files, keys and overall size on disk of the data
func (b *Bitcask) Stats() (stats Stats, err error) {
	if stats.Size, err = internal.DirSize(b.path); err != nil {
		return
	}

	b.mu.RLock()
	stats.Datafiles = len(b.datafiles)
	stats.Keys = b.trie.Size()
	b.mu.RUnlock()

	return
}

// Close closes the database and removes the lock. It is important to call
// Close() as this is the only way to cleanup the lock held by the open
// database.
func (b *Bitcask) Close() error {
	defer func() {
		b.Flock.Unlock()
		os.Remove(b.Flock.Path())
	}()

	if err := b.indexer.Save(b.trie, filepath.Join(b.path, "index")); err != nil {
		return err
	}

	for _, df := range b.datafiles {
		if err := df.Close(); err != nil {
			return err
		}
	}

	return b.curr.Close()
}

// Sync flushes all buffers to disk ensuring all data is written
func (b *Bitcask) Sync() error {
	return b.curr.Sync()
}

// Get retrieves the value of the given key. If the key is not found or an/I/O
// error occurs a null byte slice is returned along with the error.
func (b *Bitcask) Get(key []byte) ([]byte, error) {
	var df data.Datafile

	b.mu.RLock()
	value, found := b.trie.Search(key)
	if !found {
		b.mu.RUnlock()
		return nil, ErrKeyNotFound
	}

	item := value.(internal.Item)

	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}

	e, err := df.ReadAt(item.Offset, item.Size)
	b.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return nil, ErrChecksumFailed
	}

	return e.Value, nil
}

// Has returns true if the key exists in the database, false otherwise.
func (b *Bitcask) Has(key []byte) bool {
	b.mu.RLock()
	_, found := b.trie.Search(key)
	b.mu.RUnlock()
	return found
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key, value []byte) error {
	if uint32(len(key)) > b.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if uint64(len(value)) > b.config.MaxValueSize {
		return ErrValueTooLarge
	}

	b.mu.Lock()
	offset, n, err := b.put(key, value)
	if err != nil {
		b.mu.Unlock()
		return err
	}

	if b.config.Sync {
		if err := b.curr.Sync(); err != nil {
			b.mu.Unlock()
			return err
		}
	}

	item := internal.Item{FileID: b.curr.FileID(), Offset: offset, Size: n}
	b.trie.Insert(key, item)
	b.mu.Unlock()

	return nil
}

// Delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) Delete(key []byte) error {
	b.mu.Lock()
	_, _, err := b.put(key, []byte{})
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.trie.Delete(key)
	b.mu.Unlock()

	return nil
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) (err error) {
	b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
		// Skip the root node
		if len(node.Key()) == 0 {
			return true
		}

		if err = f(node.Key()); err != nil {
			return false
		}
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
			ch <- node.Key()
		}
		close(ch)
	}()

	return ch
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error returned.
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

func (b *Bitcask) put(key, value []byte) (int64, int64, error) {
	size := b.curr.Size()
	if size >= int64(b.config.MaxDatafileSize) {
		err := b.curr.Close()
		if err != nil {
			return -1, 0, err
		}

		id := b.curr.FileID()

		df, err := data.NewDatafile(b.path, id, true, b.config.MaxKeySize, b.config.MaxValueSize)
		if err != nil {
			return -1, 0, err
		}

		b.datafiles[id] = df

		id = b.curr.FileID() + 1
		curr, err := data.NewDatafile(b.path, id, false, b.config.MaxKeySize, b.config.MaxValueSize)
		if err != nil {
			return -1, 0, err
		}
		b.curr = curr
	}

	e := internal.NewEntry(key, value)
	return b.curr.Write(e)
}

func (b *Bitcask) Reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	datafiles, lastID, err := loadDatafiles(b.path, b.config.MaxKeySize, b.config.MaxValueSize)
	if err != nil {
		return err
	}

	t, err := loadIndex(b.path, b.indexer, b.config.MaxKeySize, datafiles)
	if err != nil {
		return err
	}

	curr, err := data.NewDatafile(b.path, lastID, false, b.config.MaxKeySize, b.config.MaxValueSize)
	if err != nil {
		return err
	}

	b.trie = t
	b.curr = curr
	b.datafiles = datafiles

	return nil
}

// Merge merges all datafiles in the database. Old keys are squashed
// and deleted keys removes. Duplicate key/value pairs are also removed.
// Call this function periodically to reclaim disk space.
func (b *Bitcask) Merge() error {
	// Temporary merged database path
	temp, err := ioutil.TempDir(b.path, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)

	// Create a merged database
	mdb, err := Open(temp, b.options...)
	if err != nil {
		return err
	}

	// Rewrite all key/value pairs into merged database
	// Doing this automatically strips deleted keys and
	// old key/value pairs
	err = b.Fold(func(key []byte) error {
		value, err := b.Get(key)
		if err != nil {
			return err
		}

		if err := mdb.Put(key, value); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = mdb.Close()
	if err != nil {
		return err
	}

	// Close the database
	err = b.Close()
	if err != nil {
		return err
	}

	// Remove all data files
	files, err := ioutil.ReadDir(b.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			err := os.RemoveAll(path.Join([]string{b.path, file.Name()}...))
			if err != nil {
				return err
			}
		}
	}

	// Rename all merged data files
	files, err = ioutil.ReadDir(mdb.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		err := os.Rename(
			path.Join([]string{mdb.path, file.Name()}...),
			path.Join([]string{b.path, file.Name()}...),
		)
		if err != nil {
			return err
		}
	}

	// And finally reopen the database
	return b.Reopen()
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg *config.Config
		err error
	)

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	configPath := filepath.Join(path, "config.json")
	if internal.Exists(configPath) {
		cfg, err = config.Load(configPath)
		if err != nil {
			return nil, err
		}
	} else {
		cfg = newDefaultConfig()
	}

	bitcask := &Bitcask{
		Flock:   flock.New(filepath.Join(path, "lock")),
		config:  cfg,
		options: options,
		path:    path,
		indexer: index.NewIndexer(),
	}

	for _, opt := range options {
		if err := opt(bitcask.config); err != nil {
			return nil, err
		}
	}

	locked, err := bitcask.Flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, ErrDatabaseLocked
	}

	if err := cfg.Save(configPath); err != nil {
		return nil, err
	}

	if err := bitcask.Reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}

func loadDatafiles(path string, maxKeySize uint32, maxValueSize uint64) (datafiles map[int]data.Datafile, lastID int, err error) {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, 0, err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return nil, 0, err
	}

	datafiles = make(map[int]data.Datafile, len(ids))
	for _, id := range ids {
		datafiles[id], err = data.NewDatafile(path, id, true, maxKeySize, maxValueSize)
		if err != nil {
			return
		}

	}
	if len(ids) > 0 {
		lastID = ids[len(ids)-1]
	}
	return
}

func getSortedDatafiles(datafiles map[int]data.Datafile) []data.Datafile {
	out := make([]data.Datafile, len(datafiles))
	idx := 0
	for _, df := range datafiles {
		out[idx] = df
		idx++
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].FileID() < out[j].FileID()
	})
	return out
}

func loadIndex(path string, indexer index.Indexer, maxKeySize uint32, datafiles map[int]data.Datafile) (art.Tree, error) {
	t, found, err := indexer.Load(filepath.Join(path, "index"), maxKeySize)
	if err != nil {
		return nil, err
	}
	if !found {
		sortedDatafiles := getSortedDatafiles(datafiles)
		for _, df := range sortedDatafiles {
			var offset int64
			for {
				e, n, err := df.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				// Tombstone value  (deleted key)
				if len(e.Value) == 0 {
					t.Delete(e.Key)
					offset += n
					continue
				}
				item := internal.Item{FileID: df.FileID(), Offset: offset, Size: n}
				t.Insert(e.Key, item)
				offset += n
			}
		}
	}
	return t, nil
}
