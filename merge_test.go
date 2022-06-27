package bitcaskdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir, WithMaxDatafileSize(32))
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
			err := db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		s1, err := db.Stats()
		assert.NoError(err)
		assert.Equal(0, s1.Datafiles)
		assert.Equal(1, s1.Keys)

		t.Run("PutBytes", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				err := db.PutBytes([]byte("foo"), []byte("bar"))
				assert.NoError(err)
			}
		})

		s2, err := db.Stats()
		assert.NoError(err)
		assert.Equal(5, s2.Datafiles)
		assert.Equal(1, s2.Keys)
		assert.True(s2.Size > s1.Size)

		t.Run("Merge", func(t *testing.T) {
			err := db.Merge()
			assert.NoError(err)
		})

		s3, err := db.Stats()
		assert.NoError(err)
		assert.Equal(2, s3.Datafiles)
		assert.Equal(1, s3.Keys)
		assert.True(s3.Size > s1.Size)
		assert.True(s3.Size < s2.Size)

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestMergeErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("RemoveDatabaseDirectory", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		assert.NoError(os.RemoveAll(testdir))

		err = db.Merge()
		assert.Error(err)
	})
}

func TestMergeLockingAfterMerge(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	db, err := Open(testdir)
	assert.NoError(err)
	defer db.Close()

	_, err = Open(testdir)
	assert.Error(err)

	err = db.Merge()
	assert.NoError(err)

	// This should still error.
	_, err = Open(testdir)
	assert.Error(err)
}
