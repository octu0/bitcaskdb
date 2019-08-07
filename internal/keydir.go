package internal

import (
	"bytes"
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type Item struct {
	FileID int
	Offset int64
	Size   int64
}

type Keydir struct {
	sync.RWMutex
	keys  map[uint64][]byte
	items map[uint64]Item
}

func NewKeydir() *Keydir {
	return &Keydir{
		keys:  make(map[uint64][]byte),
		items: make(map[uint64]Item),
	}
}

func (k *Keydir) Add(key []byte, fileid int, offset, size int64) Item {
	item := Item{
		FileID: fileid,
		Offset: offset,
		Size:   size,
	}

	hash := Hash(key)

	k.Lock()
	k.keys[hash] = key
	k.items[hash] = item
	k.Unlock()

	return item
}

func (k *Keydir) Get(key []byte) (Item, bool) {
	k.RLock()
	item, ok := k.items[Hash(key)]
	k.RUnlock()
	return item, ok
}

func (k *Keydir) Delete(key []byte) {
	hash := Hash(key)
	k.Lock()
	delete(k.keys, hash)
	delete(k.items, hash)
	k.Unlock()
}

func (k *Keydir) Len() int {
	return len(k.keys)
}

func (k *Keydir) Keys() chan []byte {
	ch := make(chan []byte)
	go func() {
		k.RLock()
		for _, key := range k.keys {
			ch <- key
		}
		close(ch)
		k.RUnlock()
	}()
	return ch
}

func (k *Keydir) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(k.keys); err != nil {
		return nil, err
	}
	if err := enc.Encode(k.items); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *Keydir) Load(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	if err := dec.Decode(&k.keys); err != nil {
		return err
	}
	if err := dec.Decode(&k.items); err != nil {
		return err
	}

	return nil
}

func (k *Keydir) Save(fn string) error {
	data, err := k.Bytes()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fn, data, 0644)
}

func NewKeydirFromBytes(r io.Reader) (*Keydir, error) {
	k := NewKeydir()
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&k.keys); err != nil {
		return nil, err
	}
	if err := dec.Decode(&k.items); err != nil {
		return nil, err
	}
	return k, nil
}
