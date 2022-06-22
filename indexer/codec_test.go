package indexer

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"testing"
	"time"

	"github.com/pkg/errors"
	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/octu0/bitcaskdb/runtime"
)

const (
	b64Filer = "AAAABWFiY2QxAAAAAAAAAAAAAAAAAAAAAAAAAAUAAAAGYWJjZTIyAAAAAQAAAAAAAAABAAAAAAAAAAYAAAAHYWJjZjMzMwAAAAIAAAAAAAAAAgAAAAAAAAAHAAAACGFiZ2Q0NDQ0AAAAAwAAAAAAAAADAAAAAAAAAAg="
	b64TTL   = "AAAABWFiY2QxAAAAAF91HAAAAAAGYWJjZTIyAAAAAF91HAAAAAAHYWJjZjMzMwAAAABfdRwAAAAACGFiZ2Q0NDQ0AAAAAF91HAA="
)

func TestWriteFilerIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	at, expectedSerializedSize := getFilterTree()

	out := bytes.NewBuffer(nil)
	if err := writeFilerIndex(ctx, out, at); err != nil {
		t.Errorf("writing index failed: %+v", err)
	}
	if out.Len() != expectedSerializedSize {
		t.Errorf("incorrect size of serialied index: expected %d, got: %d", expectedSerializedSize, out.Len())
	}
	//println(base64.StdEncoding.EncodeToString(out.Bytes()))
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64Filer)
	if bytes.Equal(out.Bytes(), sampleTreeBytes) != true {
		t.Errorf("unexpected serialization of the tree")
	}
}

func TestWriteTTLIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	at, expectedSerializedSize := getTTLTree()

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

func TestReadFilerIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64Filer)
	b := bytes.NewBuffer(sampleTreeBytes)

	at := art.New()
	if err := readFilerIndex(ctx, b, at); err != nil {
		t.Errorf("error while deserializing correct sample tree: %+v", err)
	}

	atsample, _ := getFilterTree()
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

func TestReadTTLIndex(t *testing.T) {
	ctx := runtime.DefaultContext()
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(b64TTL)
	b := bytes.NewBuffer(sampleTreeBytes)

	at := art.New()
	if err := readTTLIndex(ctx, b, at); err != nil {
		t.Errorf("error while deserializing correct sample tree: %+v", err)
	}

	atsample, _ := getTTLTree()
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
		if err := writeFiler(b, Filer{0, 0, 0}); err != nil {
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

func getFilterTree() (art.Tree, int) {
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
			FileID: int32(i),
			Index:  int64(i),
			Size:   int64(len(key)),
		})
		expectedSerializedSize += 4        // uint32 : keySize
		expectedSerializedSize += len(key) // : dataSize
		expectedSerializedSize += 4        // uint32 : fileIDSize
		expectedSerializedSize += 8        // uint64 : offsetSize
		expectedSerializedSize += 8        // uint64 : sizeSize
	}

	return at, expectedSerializedSize
}

func getTTLTree() (art.Tree, int) {
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
