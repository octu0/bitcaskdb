package bitcask

import (
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofrs/flock"
	"github.com/prologic/trie"

	"github.com/prologic/bitcask/internal"
)

var (
	ErrKeyNotFound    = errors.New("error: key not found")
	ErrKeyTooLarge    = errors.New("error: key too large")
	ErrValueTooLarge  = errors.New("error: value too large")
	ErrChecksumFailed = errors.New("error: checksum failed")
	ErrDatabaseLocked = errors.New("error: database locked")
)

type Bitcask struct {
	*flock.Flock

	config    *config
	path      string
	curr      *internal.Datafile
	keydir    *internal.Keydir
	datafiles []*internal.Datafile
	trie      *trie.Trie

	maxDatafileSize int64
}

func (b *Bitcask) Close() error {
	defer func() {
		b.Flock.Unlock()
		os.Remove(b.Flock.Path())
	}()

	for _, df := range b.datafiles {
		df.Close()
	}
	return b.curr.Close()
}

func (b *Bitcask) Sync() error {
	return b.curr.Sync()
}

func (b *Bitcask) Get(key string) ([]byte, error) {
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

	e, err := df.ReadAt(item.Offset)
	if err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return nil, ErrChecksumFailed
	}

	return e.Value, nil
}

func (b *Bitcask) Has(key string) bool {
	_, ok := b.keydir.Get(key)
	return ok
}

func (b *Bitcask) Put(key string, value []byte) error {
	if len(key) > b.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > b.config.MaxValueSize {
		return ErrValueTooLarge
	}

	offset, err := b.put(key, value)
	if err != nil {
		return err
	}

	item := b.keydir.Add(key, b.curr.FileID(), offset)
	b.trie.Add(key, item)

	return nil
}

func (b *Bitcask) Delete(key string) error {
	_, err := b.put(key, []byte{})
	if err != nil {
		return err
	}

	b.keydir.Delete(key)
	b.trie.Remove(key)

	return nil
}

func (b *Bitcask) Scan(prefix string, f func(key string) error) error {
	keys := b.trie.PrefixSearch(prefix)
	for _, key := range keys {
		if err := f(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitcask) Keys() chan string {
	return b.keydir.Keys()
}

func (b *Bitcask) Fold(f func(key string) error) error {
	for key := range b.keydir.Keys() {
		if err := f(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bitcask) put(key string, value []byte) (int64, error) {
	size, err := b.curr.Size()
	if err != nil {
		return -1, err
	}

	if size >= b.maxDatafileSize {
		err := b.curr.Close()
		if err != nil {
			return -1, err
		}

		df, err := internal.NewDatafile(b.path, b.curr.FileID(), true)
		if err != nil {
			return -1, err
		}

		b.datafiles = append(b.datafiles, df)

		id := b.curr.FileID() + 1
		curr, err := internal.NewDatafile(b.path, id, false)
		if err != nil {
			return -1, err
		}
		b.curr = curr
	}

	e := internal.NewEntry(key, value)
	return b.curr.Write(e)
}

func (b *Bitcask) setMaxDatafileSize(size int64) error {
	b.maxDatafileSize = size
	return nil
}

func Merge(path string, force bool) error {
	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return err
	}

	// Do not merge if we only have 1 Datafile
	if len(ids) <= 1 {
		return nil
	}

	// Don't merge the Active Datafile (the last one)
	fns = fns[:len(fns)-1]
	ids = ids[:len(ids)-1]

	temp, err := ioutil.TempDir("", "bitcask")
	if err != nil {
		return err
	}

	for i, fn := range fns {
		// Don't merge Datafiles whose .hint files we've already generated
		// (they are already merged); unless we set the force flag to true
		// (forcing a re-merge).
		if filepath.Ext(fn) == ".hint" && !force {
			// Already merged
			continue
		}

		id := ids[i]

		keydir := internal.NewKeydir()

		df, err := internal.NewDatafile(path, id, true)
		if err != nil {
			return err
		}
		defer df.Close()

		for {
			e, err := df.Read()
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

			keydir.Add(e.Key, ids[i], e.Offset)
		}

		tempdf, err := internal.NewDatafile(temp, id, false)
		if err != nil {
			return err
		}
		defer tempdf.Close()

		for key := range keydir.Keys() {
			item, _ := keydir.Get(key)
			e, err := df.ReadAt(item.Offset)
			if err != nil {
				return err
			}

			_, err = tempdf.Write(e)
			if err != nil {
				return err
			}
		}

		err = tempdf.Close()
		if err != nil {
			return err
		}

		err = df.Close()
		if err != nil {
			return err
		}

		err = os.Rename(tempdf.Name(), df.Name())
		if err != nil {
			return err
		}

		hint := strings.TrimSuffix(df.Name(), ".data") + ".hint"
		err = keydir.Save(hint)
		if err != nil {
			return err
		}
	}

	return nil
}

func Open(path string, options ...option) (*Bitcask, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	err := Merge(path, false)
	if err != nil {
		return nil, err
	}

	fns, err := internal.GetDatafiles(path)
	if err != nil {
		return nil, err
	}

	ids, err := internal.ParseIds(fns)
	if err != nil {
		return nil, err
	}

	var datafiles []*internal.Datafile

	keydir := internal.NewKeydir()
	trie := trie.New()

	for i, fn := range fns {
		df, err := internal.NewDatafile(path, ids[i], true)
		if err != nil {
			return nil, err
		}
		datafiles = append(datafiles, df)

		if filepath.Ext(fn) == ".hint" {
			f, err := os.Open(filepath.Join(path, fn))
			if err != nil {
				return nil, err
			}
			defer f.Close()

			hint, err := internal.NewKeydirFromBytes(f)
			if err != nil {
				return nil, err
			}

			for key := range hint.Keys() {
				item, _ := hint.Get(key)
				_ = keydir.Add(key, item.FileID, item.Offset)
				trie.Add(key, item)
			}
		} else {
			for {
				e, err := df.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				// Tombstone value  (deleted key)
				if len(e.Value) == 0 {
					keydir.Delete(e.Key)
					continue
				}

				item := keydir.Add(e.Key, ids[i], e.Offset)
				trie.Add(e.Key, item)
			}
		}
	}

	var id int
	if len(ids) > 0 {
		id = ids[(len(ids) - 1)]
	}

	curr, err := internal.NewDatafile(path, id, false)
	if err != nil {
		return nil, err
	}

	bitcask := &Bitcask{
		Flock:     flock.New(filepath.Join(path, "lock")),
		config:    newDefaultConfig(),
		path:      path,
		curr:      curr,
		keydir:    keydir,
		datafiles: datafiles,
		trie:      trie,

		maxDatafileSize: DefaultMaxDatafileSize,
	}

	for _, opt := range options {
		err = opt(bitcask.config)
		if err != nil {
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

	return bitcask, nil
}
