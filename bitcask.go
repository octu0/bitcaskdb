package bitcask

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/prologic/trie"
)

type Bitcask struct {
	*flock.Flock

	opts      Options
	path      string
	curr      *Datafile
	keydir    *Keydir
	datafiles []*Datafile
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
	var df *Datafile

	item, ok := b.keydir.Get(key)
	if !ok {
		return nil, fmt.Errorf("error: key not found %s", key)
	}

	if item.FileID == b.curr.id {
		df = b.curr
	} else {
		df = b.datafiles[item.FileID]
	}

	e, err := df.ReadAt(item.Index)
	if err != nil {
		return nil, err
	}

	return e.Value, nil
}

func (b *Bitcask) Put(key string, value []byte) error {
	if len(key) > b.opts.MaxKeySize {
		return fmt.Errorf("error: key too large %d > %d", len(key), b.opts.MaxKeySize)
	}
	if len(value) > b.opts.MaxValueSize {
		return fmt.Errorf("error: value too large %d > %d", len(value), b.opts.MaxValueSize)
	}

	index, err := b.put(key, value)
	if err != nil {
		return err
	}

	item := b.keydir.Add(key, b.curr.id, index, time.Now().Unix())
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

		df, err := NewDatafile(b.path, b.curr.id, true)
		b.datafiles = append(b.datafiles, df)

		id := b.curr.id + 1
		curr, err := NewDatafile(b.path, id, false)
		if err != nil {
			return -1, err
		}
		b.curr = curr
	}

	e := NewEntry(key, value)
	return b.curr.Write(e)
}

func (b *Bitcask) setMaxDatafileSize(size int64) error {
	b.maxDatafileSize = size
	return nil
}

func Merge(path string, force bool) error {
	fns, err := getDatafiles(path)
	if err != nil {
		return err
	}

	ids, err := parseIds(fns)
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

		keydir := NewKeydir()

		df, err := NewDatafile(path, id, true)
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

			keydir.Add(e.Key, ids[i], e.Index, e.Timestamp)
		}

		tempdf, err := NewDatafile(temp, id, false)
		if err != nil {
			return err
		}
		defer tempdf.Close()

		for key := range keydir.Keys() {
			item, _ := keydir.Get(key)
			e, err := df.ReadAt(item.Index)
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

func Open(path string, options ...func(*Bitcask) error) (*Bitcask, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	err := Merge(path, false)
	if err != nil {
		return nil, err
	}

	fns, err := getDatafiles(path)
	if err != nil {
		return nil, err
	}

	ids, err := parseIds(fns)
	if err != nil {
		return nil, err
	}

	var datafiles []*Datafile

	keydir := NewKeydir()
	trie := trie.New()

	for i, fn := range fns {
		df, err := NewDatafile(path, ids[i], true)
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

			hint, err := NewKeydirFromBytes(f)
			if err != nil {
				return nil, err
			}

			for key := range hint.Keys() {
				item, _ := hint.Get(key)
				_ = keydir.Add(key, item.FileID, item.Index, item.Timestamp)
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

				item := keydir.Add(e.Key, ids[i], e.Index, e.Timestamp)
				trie.Add(e.Key, item)
			}
		}
	}

	var id int
	if len(ids) > 0 {
		id = ids[(len(ids) - 1)]
	}

	curr, err := NewDatafile(path, id, false)
	if err != nil {
		return nil, err
	}

	bitcask := &Bitcask{
		Flock:     flock.New(filepath.Join(path, "lock")),
		opts:      NewDefaultOptions(),
		path:      path,
		curr:      curr,
		keydir:    keydir,
		datafiles: datafiles,
		trie:      trie,

		maxDatafileSize: DefaultMaxDatafileSize,
	}

	for _, option := range options {
		err = option(bitcask)
		if err != nil {
			return nil, err
		}
	}

	locked, err := bitcask.Flock.TryLock()
	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, fmt.Errorf("error: database locked %s", path)
	}

	return bitcask, nil
}
