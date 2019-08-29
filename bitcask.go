package bitcask

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
	"github.com/plar/go-adaptive-radix-tree"

	"github.com/prologic/bitcask/internal"
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

	config    *config
	options   []Option
	path      string
	curr      *internal.Datafile
	keydir    *internal.Keydir
	datafiles map[int]*internal.Datafile
	trie      art.Tree
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
	var size int64

	size, err = internal.DirSize(b.path)
	if err != nil {
		return
	}

	stats.Datafiles = len(b.datafiles)
	stats.Keys = b.keydir.Len()
	stats.Size = size

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

	if err := b.keydir.Save(path.Join(b.path, "index")); err != nil {
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
	var df *internal.Datafile

	item, ok := b.keydir.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	if item.FileID == b.curr.FileID() {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}

	e, err := df.ReadAt(item.Offset, item.Size)
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
	_, ok := b.keydir.Get(key)
	return ok
}

// Put stores the key and value in the database.
func (b *Bitcask) Put(key, value []byte) error {
	if len(key) > b.config.maxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > b.config.maxValueSize {
		return ErrValueTooLarge
	}

	offset, n, err := b.put(key, value)
	if err != nil {
		return err
	}

	if b.config.sync {
		if err := b.curr.Sync(); err != nil {
			return err
		}
	}

	item := b.keydir.Add(key, b.curr.FileID(), offset, n)

	if b.config.greedyScan {
		b.trie.Insert(key, item)
	}

	return nil
}

// Delete deletes the named key. If the key doesn't exist or an I/O error
// occurs the error is returned.
func (b *Bitcask) Delete(key []byte) error {
	_, _, err := b.put(key, []byte{})
	if err != nil {
		return err
	}

	b.keydir.Delete(key)

	if b.config.greedyScan {
		b.trie.Delete(key)
	}

	return nil
}

// Scan performs a prefix scan of keys matching the given prefix and calling
// the function `f` with the keys found. If the function returns an error
// no further keys are processed and the first error returned.
func (b *Bitcask) Scan(prefix []byte, f func(key []byte) error) error {
	if b.config.greedyScan {
		b.trie.ForEachPrefix(prefix, func(node art.Node) bool {
			if err := f(node.Key()); err != nil {
				return false
			}
			return true
		})
		return nil
	}

	keys := b.Keys()
	for key := range keys {
		if bytes.Equal(prefix, key[:len(prefix)]) {
			if err := f([]byte(key)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Len returns the total number of keys in the database
func (b *Bitcask) Len() int {
	return b.keydir.Len()
}

// Keys returns all keys in the database as a channel of string(s)
func (b *Bitcask) Keys() chan []byte {
	return b.keydir.Keys()
}

// Fold iterates over all keys in the database calling the function `f` for
// each key. If the function returns an error, no further keys are processed
// and the error returned.
func (b *Bitcask) Fold(f func(key []byte) error) error {
	for key := range b.keydir.Keys() {
		if err := f(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitcask) put(key, value []byte) (int64, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := b.curr.Size()
	if size >= int64(b.config.maxDatafileSize) {
		err := b.curr.Close()
		if err != nil {
			return -1, 0, err
		}

		id := b.curr.FileID()

		df, err := internal.NewDatafile(b.path, id, true)
		if err != nil {
			return -1, 0, err
		}

		b.datafiles[id] = df

		id = b.curr.FileID() + 1
		curr, err := internal.NewDatafile(b.path, id, false)
		if err != nil {
			return -1, 0, err
		}
		b.curr = curr
	}

	e := internal.NewEntry(key, value)
	return b.curr.Write(e)
}

func (b *Bitcask) readConfig() error {
	if internal.Exists(filepath.Join(b.path, "config.json")) {
		data, err := ioutil.ReadFile(filepath.Join(b.path, "config.json"))
		if err != nil {
			return err
		}

		if err := json.Unmarshal(data, &b.config); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitcask) writeConfig() error {
	data, err := json.Marshal(b.config)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(b.path, "config.json"), data, 0600)
}

func (b *Bitcask) reopen() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	fns, err := internal.GetDatafiles(b.path)
	if err != nil {
		return err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return err
	}

	datafiles := make(map[int]*internal.Datafile, len(ids))

	for _, id := range ids {
		df, err := internal.NewDatafile(b.path, id, true)
		if err != nil {
			return err
		}
		datafiles[id] = df
	}

	keydir := internal.NewKeydir()

	var t art.Tree
	if b.config.greedyScan {
		t = art.New()
	}

	if internal.Exists(path.Join(b.path, "index")) {
		if err := keydir.Load(path.Join(b.path, "index")); err != nil {
			return err
		}
		for key := range keydir.Keys() {
			item, _ := keydir.Get(key)
			if b.config.greedyScan {
				t.Insert(key, item)
			}
		}
	} else {
		for i, df := range datafiles {
			for {
				e, n, err := df.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				// Tombstone value  (deleted key)
				if len(e.Value) == 0 {
					keydir.Delete(e.Key)
					continue
				}

				item := keydir.Add(e.Key, ids[i], e.Offset, n)
				if b.config.greedyScan {
					t.Insert(e.Key, item)
				}
			}
		}
	}

	var id int
	if len(ids) > 0 {
		id = ids[(len(ids) - 1)]
	}

	curr, err := internal.NewDatafile(b.path, id, false)
	if err != nil {
		return err
	}

	b.curr = curr
	b.datafiles = datafiles

	b.keydir = keydir

	if b.config.greedyScan {
		b.trie = t
	}

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
	return b.reopen()
}

// Open opens the database at the given path with optional options.
// Options can be provided with the `WithXXX` functions that provide
// configuration options as functions.
func Open(path string, options ...Option) (*Bitcask, error) {
	var (
		cfg *config
		err error
	)

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	cfg, err = getConfig(path)
	if err != nil {
		cfg = newDefaultConfig()
	}

	bitcask := &Bitcask{
		Flock:   flock.New(filepath.Join(path, "lock")),
		config:  cfg,
		options: options,
		path:    path,
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

	if err := bitcask.writeConfig(); err != nil {
		return nil, err
	}

	if err := bitcask.reopen(); err != nil {
		return nil, err
	}

	return bitcask, nil
}
