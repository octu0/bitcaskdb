package index

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"
	assert2 "github.com/stretchr/testify/assert"
)

func Test_TTLIndexer(t *testing.T) {
	assert := assert2.New(t)
	tempDir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)
	defer os.RemoveAll(tempDir)

	currTime := time.Date(2020, 12, 27, 0, 0, 0, 0, time.UTC)
	trie := art.New()

	t.Run("LoadEmpty", func(t *testing.T) {
		newTrie, found, err := NewTTLIndexer().Load(filepath.Join(tempDir, "ttl_index"), 4)
		assert.NoError(err)
		assert.False(found)
		assert.Equal(trie, newTrie)
	})

	t.Run("Save", func(t *testing.T) {
		trie.Insert([]byte("key"), currTime)
		err := NewTTLIndexer().Save(trie, filepath.Join(tempDir, "ttl_index"))
		assert.NoError(err)
		trie.Insert([]byte("foo"), currTime.Add(24*time.Hour))
		err = NewTTLIndexer().Save(trie, filepath.Join(tempDir, "ttl_index"))
		assert.NoError(err)
		trie.Insert([]byte("key"), currTime.Add(-24*time.Hour))
		err = NewTTLIndexer().Save(trie, filepath.Join(tempDir, "ttl_index"))
		assert.NoError(err)
	})

	t.Run("Load", func(t *testing.T) {
		newTrie, found, err := NewTTLIndexer().Load(filepath.Join(tempDir, "ttl_index"), 4)
		assert.NoError(err)
		assert.True(found)
		assert.Equal(2, newTrie.Size())
		value, found := newTrie.Search([]byte("key"))
		assert.True(found)
		assert.Equal(currTime.Add(-24*time.Hour), value)
		value, found = newTrie.Search([]byte("foo"))
		assert.True(found)
		assert.Equal(currTime.Add(24*time.Hour), value)
	})
}
