package indexer

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/runtime"
)

const (
	b64TTL string = "AAAABWFiY2QxAAAAAF91HAAAAAAGYWJjZTIyAAAAAF91HAAAAAAHYWJjZjMzMwAAAABfdRwAAAAACGFiZ2Q0NDQ0AAAAAF91HAA="
)

func testTTLTree() (art.Tree, int) {
	at := art.New()
	keys := [][]byte{
		[]byte("abcd1"),
		[]byte("abce22"),
		[]byte("abcf333"),
		[]byte("abgd4444"),
	}

	expectedSerializedSize := 0
	for _, key := range keys {
		at.Insert(key, time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC))
		expectedSerializedSize += 4        // uint32 : keySize
		expectedSerializedSize += len(key) // : dataSize
		expectedSerializedSize += 8        // uint64 : expiry
	}

	return at, expectedSerializedSize
}

func TestWriteTTLIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	at, expectedSerializedSize := testTTLTree()

	out := bytes.NewBuffer(nil)
	if err := writeTTLIndex(ctx, out, at); err != nil {
		t.Errorf("writing index failed: %+v", err)
	}
	if out.Len() != expectedSerializedSize {
		t.Errorf("incorrect size of serialied index: expected %d, got: %d", expectedSerializedSize, out.Len())
	}
	//println(base64.StdEncoding.EncodeToString(out.Bytes()))
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64TTL)
	if bytes.Equal(out.Bytes(), sampleTreeBytes) != true {
		t.Errorf("unexpected serialization of the tree")
	}
}

func TestReadTTLIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64TTL)
	b := bytes.NewBuffer(sampleTreeBytes)

	at := art.New()
	if err := readTTLIndex(ctx, b, at); err != nil {
		t.Errorf("error while deserializing correct sample tree: %+v", err)
	}

	atsample, _ := testTTLTree()
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

func TestTTLReadCorruptedData(t *testing.T) {
	ctx := runtime.DefaultContext()
	t.Run("keysize/0", func(tt *testing.T) {
		b := bytes.NewBuffer(nil)
		if err := binary.Write(b, binary.BigEndian, uint32(0)); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if _, err := b.Write([]byte("test")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if err := writeTTL(b, time.Time{}); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		at := art.New()
		err := readTTLIndex(ctx, b, at)
		if err == nil {
			tt.Errorf("must error")
		}
		if errors.Is(err, errTruncatedKeySize) != true {
			tt.Errorf("errTruncatedKeySize! %+v", err)
		}
	})
}

func TestTTLIndexer(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "bitcask")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	defer os.RemoveAll(tempDir)

	currTime := time.Date(2020, 12, 27, 0, 0, 0, 0, time.UTC)
	trie := art.New()

	t.Run("LoadEmpty", func(tt *testing.T) {
		newTrie, found, err := NewTTLIndexer(runtime.DefaultContext()).Load(filepath.Join(tempDir, "ttl_index"))
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
		if err := NewTTLIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "ttl_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("foo"), currTime.Add(24*time.Hour))
		if err := NewTTLIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "ttl_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		trie.Insert([]byte("key"), currTime.Add(-24*time.Hour))
		if err := NewTTLIndexer(runtime.DefaultContext()).Save(trie, filepath.Join(tempDir, "ttl_index")); err != nil {
			tt.Errorf("no error: %+v", err)
		}
	})

	t.Run("Load", func(tt *testing.T) {
		newTrie, found, err := NewTTLIndexer(runtime.DefaultContext()).Load(filepath.Join(tempDir, "ttl_index"))
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
