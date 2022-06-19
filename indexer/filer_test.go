package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/runtime"
)

func TestFilerIndexer(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "bitcask")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	defer os.RemoveAll(tempDir)

	trie := art.New()

	t.Run("LoadEmpty", func(tt *testing.T) {
		newTrie, found, err := NewFilerIndexer(runtime.DefaultContext()).Load(filepath.Join(tempDir, "filer_index"))
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
		trie.Insert([]byte("key"), Filer{0, 0, 20})
		if err := NewFilerIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "filer_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("foo"), Filer{1, 8, 15})
		if err := NewFilerIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "filer_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("key"), Filer{0, 24, 30})
		if err := NewFilerIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "filer_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
	})

	t.Run("Load", func(tt *testing.T) {
		newTrie, found, err := NewFilerIndexer(runtime.DefaultContext()).Load(filepath.Join(tempDir, "filer_index"))
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
			f := value.(Filer)
			if (f.FileID == 0 && f.Index == 24 && f.Size == 30) != true {
				tt.Errorf("got updated Filer{}")
			}
		}
		if value, found := newTrie.Search([]byte("foo")); found != true {
			tt.Errorf("'foo' exists!")
		} else {
			f := value.(Filer)
			if (f.FileID == 1 && f.Index == 8 && f.Size == 15) != true {
				tt.Errorf("got updated Filer{}")
			}
		}
	})
}
