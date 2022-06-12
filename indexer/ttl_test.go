package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/context"
)

func TestTTLIndexer(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "bitcask")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	defer os.RemoveAll(tempDir)

	currTime := time.Date(2020, 12, 27, 0, 0, 0, 0, time.UTC)
	trie := art.New()

	t.Run("LoadEmpty", func(tt *testing.T) {
		newTrie, found, err := NewTTLIndexer(context.Default()).Load(filepath.Join(tempDir, "ttl_index"))
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if found {
			tt.Errorf("not found")
		}
		if (trie.Size() == 0 && newTrie.Size() == 0) != true {
			tt.Errorf("empty trie")
		}
	})

	t.Run("Save", func(tt *testing.T) {
		trie.Insert([]byte("key"), currTime)
		if err := NewTTLIndexer(context.Default()).Save(trie, filepath.Join(tempDir, "ttl_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("foo"), currTime.Add(24*time.Hour))
		if err := NewTTLIndexer(context.Default()).Save(trie, filepath.Join(tempDir, "ttl_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("key"), currTime.Add(-24*time.Hour))
		if err := NewTTLIndexer(context.Default()).Save(trie, filepath.Join(tempDir, "ttl_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
	})

	t.Run("Load", func(tt *testing.T) {
		newTrie, found, err := NewTTLIndexer(context.Default()).Load(filepath.Join(tempDir, "ttl_index"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if found != true {
			tt.Errorf("found!")
		}
		if newTrie.Size() != 2 {
			tt.Errorf("2 keys: %d", newTrie.Size())
		}
		if value, found := newTrie.Search([]byte("key")); found != true {
			tt.Errorf("'key' exists!")
		} else {
			if currTime.Add(-24*time.Hour).Equal(value.(time.Time)) != true {
				tt.Errorf("same time")
			}
		}
		if value, found := newTrie.Search([]byte("foo")); found != true {
			tt.Errorf("'foo' exists!")
		} else {
			if currTime.Add(24*time.Hour).Equal(value.(time.Time)) != true {
				tt.Errorf("same time")
			}
		}
	})
}
