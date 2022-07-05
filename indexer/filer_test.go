package indexer

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/runtime"
)

const (
	b64Filer string = "AAAABWFiY2QxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAUAAAAGYWJjZTIyAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAYAAAAHYWJjZjMzMwAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAHAAAACGFiZ2Q0NDQ0AAAAAAAAAAMAAAAAAAAAAwAAAAAAAAADAAAAAAAAAAg="
)

func testFilterTree() (art.Tree, int) {
	at := art.New()
	keys := [][]byte{
		[]byte("abcd1"),
		[]byte("abce22"),
		[]byte("abcf333"),
		[]byte("abgd4444"),
	}

	expectedSerializedSize := 0
	for i, key := range keys {
		at.Insert(key, Filer{
			FileID: datafile.CreateFileID(int64(i), int64(i)),
			Index:  int64(i),
			Size:   int64(len(key)),
		})
		expectedSerializedSize += 4        // uint32 : keySize
		expectedSerializedSize += len(key) // : key
		expectedSerializedSize += 8        // uint32 : FileID.Time
		expectedSerializedSize += 8        // uint32 : FileID.Rand
		expectedSerializedSize += 8        // uint64 : offsetSize
		expectedSerializedSize += 8        // uint64 : sizeSize
	}

	return at, expectedSerializedSize
}

func TestWriteFilerIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	at, expectedSerializedSize := testFilterTree()

	out := bytes.NewBuffer(nil)
	if err := writeFilerIndex(ctx, out, at); err != nil {
		t.Errorf("writing index failed: %+v", err)
	}
	if out.Len() != expectedSerializedSize {
		t.Errorf("incorrect size of serialied index: expected %d, got: %d", expectedSerializedSize, out.Len())
	}
	t.Logf("%s", base64.StdEncoding.EncodeToString(out.Bytes()))
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64Filer)
	if bytes.Equal(out.Bytes(), sampleTreeBytes) != true {
		t.Errorf("unexpected serialization of the tree")
	}
}

func TestReadFilerIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64Filer)
	b := bytes.NewBuffer(sampleTreeBytes)

	at := art.New()
	if err := readFilerIndex(ctx, b, at); err != nil {
		t.Errorf("error while deserializing correct sample tree: %+v", err)
	}

	atsample, _ := testFilterTree()
	if atsample.Size() != at.Size() {
		t.Errorf("trees aren't the same size, expected %v, got %v", atsample.Size(), at.Size())
	}
	atsample.ForEach(func(node art.Node) bool {
		if _, found := at.Search(node.Key()); found != true {
			t.Errorf("expected node wasn't found: %s", node.Key())
		}
		return true
	})
}

func TestFilerReadCorruptedData(t *testing.T) {
	ctx := runtime.DefaultContext()
	t.Run("keysize/0", func(tt *testing.T) {
		b := bytes.NewBuffer(nil)
		if err := binary.Write(b, binary.BigEndian, uint32(0)); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if _, err := b.Write([]byte("test")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if err := writeFiler(b, Filer{datafile.CreateFileID(0, 0), 0, 0}); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		at := art.New()
		err := readFilerIndex(ctx, b, at)
		if err == nil {
			tt.Errorf("must error")
		}
		if errors.Is(err, errTruncatedKeySize) != true {
			tt.Errorf("errTruncatedKeySize! %+v", err)
		}
	})
}

func TestFilerIndexer(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "bitcask")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	defer os.RemoveAll(tempDir)

	trie := art.New()

	fileID1 := datafile.NextFileID()
	fileID2 := datafile.NextFileID()
	fileID3 := datafile.NextFileID()

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
		trie.Insert([]byte("key"), Filer{fileID1, 0, 20})

		if err := NewFilerIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "filer_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("foo"), Filer{fileID2, 8, 15})
		if err := NewFilerIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "filer_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("key"), Filer{fileID3, 24, 30})
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
			if (f.FileID.Equal(fileID3) && f.Index == 24 && f.Size == 30) != true {
				tt.Errorf("got updated Filer{}")
			}
		}
		if value, found := newTrie.Search([]byte("foo")); found != true {
			tt.Errorf("'foo' exists!")
		} else {
			f := value.(Filer)
			if (f.FileID.Equal(fileID2) && f.Index == 8 && f.Size == 15) != true {
				tt.Errorf("got updated Filer{}")
			}
		}
	})
}
