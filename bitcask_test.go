package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/prologic/bitcask/internal"
	"github.com/prologic/bitcask/internal/config"
	"github.com/prologic/bitcask/internal/mocks"
)

var (
	ErrMockError = errors.New("error: mock error")
)

type sortByteArrays [][]byte

func (b sortByteArrays) Len() int {
	return len(b)
}

func (b sortByteArrays) Less(i, j int) bool {
	switch bytes.Compare(b[i], b[j]) {
	case -1:
		return true
	case 0, 1:
		return false
	}
	return false
}

func (b sortByteArrays) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func SortByteArrays(src [][]byte) [][]byte {
	sorted := sortByteArrays(src)
	sort.Sort(sorted)
	return sorted
}

func TestAll(t *testing.T) {
	var (
		db      *Bitcask
		testdir string
		err     error
	)

	assert := assert.New(t)

	testdir, err = ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testdir)
		assert.NoError(err)
	})

	t.Run("Put", func(t *testing.T) {
		err = db.Put([]byte([]byte("foo")), []byte("bar"))
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		val, err := db.Get([]byte("foo"))
		assert.NoError(err)
		assert.Equal([]byte("bar"), val)
	})

	t.Run("Len", func(t *testing.T) {
		assert.Equal(1, db.Len())
	})

	t.Run("Has", func(t *testing.T) {
		assert.True(db.Has([]byte("foo")))
	})

	t.Run("Keys", func(t *testing.T) {
		keys := make([][]byte, 0)
		for key := range db.Keys() {
			keys = append(keys, key)
		}
		assert.Equal([][]byte{[]byte("foo")}, keys)
	})

	t.Run("Fold", func(t *testing.T) {
		var (
			keys   [][]byte
			values [][]byte
		)

		err := db.Fold(func(key []byte) error {
			value, err := db.Get(key)
			if err != nil {
				return err
			}
			keys = append(keys, key)
			values = append(values, value)
			return nil
		})
		assert.NoError(err)
		assert.Equal([][]byte{[]byte("foo")}, keys)
		assert.Equal([][]byte{[]byte("bar")}, values)
	})

	t.Run("Delete", func(t *testing.T) {
		err := db.Delete([]byte("foo"))
		assert.NoError(err)
		_, err = db.Get([]byte("foo"))
		assert.Error(err)
		assert.Equal(ErrKeyNotFound, err)
	})

	t.Run("Sync", func(t *testing.T) {
		err = db.Sync()
		assert.NoError(err)
	})

	t.Run("Close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(err)
	})
}

func TestReopen1(t *testing.T) {
	assert := assert.New(t)
	for i := 0; i < 10; i ++ {
		testdir, _ := ioutil.TempDir("", "bitcask")
		db, _ := Open(testdir, WithMaxDatafileSize(1))
		_ = db.Put([]byte("foo"), []byte("bar"))
		_ = db.Put([]byte("foo"), []byte("bar1"))
		_ = db.Put([]byte("foo"), []byte("bar2"))
		_ = db.Put([]byte("foo"), []byte("bar3"))
		_ = db.Put([]byte("foo"), []byte("bar4"))
		_ = db.Put([]byte("foo"), []byte("bar5"))
		_ = db.Reopen()
		val, _ := db.Get([]byte("foo"))
		assert.Equal("bar5", string(val))
	}
}

func TestReopen(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Reopen", func(t *testing.T) {
			err = db.Reopen()
			assert.NoError(err)
		})

		t.Run("GetAfterReopen", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("PutAfterReopen", func(t *testing.T) {
			err = db.Put([]byte("zzz"), []byte("foo"))
			assert.NoError(err)
		})

		t.Run("GetAfterReopenAndPut", func(t *testing.T) {
			val, err := db.Get([]byte("zzz"))
			assert.NoError(err)
			assert.Equal([]byte("foo"), val)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestDeletedKeys(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Delete", func(t *testing.T) {
			err := db.Delete([]byte("foo"))
			assert.NoError(err)
			_, err = db.Get([]byte("foo"))
			assert.Error(err)
			assert.Equal(ErrKeyNotFound, err)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			_, err = db.Get([]byte("foo"))
			assert.Error(err)
			assert.Equal(ErrKeyNotFound, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestConfigErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("CorruptConfig", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir)
		assert.NoError(err)
		assert.NoError(db.Close())

		assert.NoError(ioutil.WriteFile(filepath.Join(testdir, "config.json"), []byte("foo bar baz"), 0600))

		_, err = Open(testdir)
		assert.Error(err)
	})

	t.Run("BadConfigPath", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		assert.NoError(os.Mkdir(filepath.Join(testdir, "config.json"), 0700))

		_, err = Open(testdir)
		assert.Error(err)
	})
}

func TestReIndex(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})

		t.Run("DeleteIndex", func(t *testing.T) {
			err := os.Remove(filepath.Join(testdir, "index"))
			assert.NoError(err)
		})
	})

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestReIndexDeletedKeys(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Delete", func(t *testing.T) {
			err := db.Delete([]byte("foo"))
			assert.NoError(err)
			_, err = db.Get([]byte("foo"))
			assert.Error(err)
			assert.Equal(ErrKeyNotFound, err)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})

		t.Run("DeleteIndex", func(t *testing.T) {
			err := os.Remove(filepath.Join(testdir, "index"))
			assert.NoError(err)
		})
	})

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  *Bitcask
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			_, err := db.Get([]byte("foo"))
			assert.Error(err)
			assert.Equal(ErrKeyNotFound, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestSync(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testdir, WithSync(true))
		assert.NoError(err)
	})

	t.Run("Put", func(t *testing.T) {
		key := []byte(strings.Repeat(" ", 17))
		value := []byte("foobar")
		err = db.Put(key, value)
	})
}

func TestMaxKeySize(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testdir, WithMaxKeySize(16))
		assert.NoError(err)
	})

	t.Run("Put", func(t *testing.T) {
		key := []byte(strings.Repeat(" ", 17))
		value := []byte("foobar")
		err = db.Put(key, value)
		assert.Error(err)
		assert.Equal(ErrKeyTooLarge, err)
	})
}

func TestMaxValueSize(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testdir, WithMaxValueSize(16))
		assert.NoError(err)
	})

	t.Run("Put", func(t *testing.T) {
		key := []byte("foo")
		value := []byte(strings.Repeat(" ", 17))
		err = db.Put(key, value)
		assert.Error(err)
		assert.Equal(ErrValueTooLarge, err)
	})
}

func TestStats(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Stats", func(t *testing.T) {
			stats, err := db.Stats()
			assert.NoError(err)
			assert.Equal(stats.Datafiles, 0)
			assert.Equal(stats.Keys, 1)
		})

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

func TestStatsError(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		})

		t.Run("Stats", func(t *testing.T) {
			stats, err := db.Stats()
			assert.NoError(err)
			assert.Equal(stats.Datafiles, 0)
			assert.Equal(stats.Keys, 1)
		})

		t.Run("FabricatedDestruction", func(t *testing.T) {
			// This would never happen in reality :D
			// Or would it? :)
			err = os.RemoveAll(testdir)
			assert.NoError(err)
		})

		t.Run("Stats", func(t *testing.T) {
			_, err := db.Stats()
			assert.Error(err)
		})
	})
}

func TestMaxDatafileSize(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)
	defer os.RemoveAll(testdir)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir, WithMaxDatafileSize(32))
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})
	})

	t.Run("Put", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := db.Put([]byte(fmt.Sprintf("key_%d", i)), []byte("bar"))
			assert.NoError(err)
		}
	})

	t.Run("Sync", func(t *testing.T) {
		err = db.Sync()
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		val, err := db.Get([]byte("foo"))
		assert.NoError(err)
		assert.Equal([]byte("bar"), val)

		for i := 0; i < 10; i++ {
			val, err = db.Get([]byte(fmt.Sprintf("key_%d", i)))
			assert.NoError(err)
			assert.Equal([]byte("bar"), val)
		}
	})

	t.Run("Close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(err)
	})
}

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

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		s1, err := db.Stats()
		assert.NoError(err)
		assert.Equal(0, s1.Datafiles)
		assert.Equal(1, s1.Keys)

		t.Run("Put", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				err := db.Put([]byte("foo"), []byte("bar"))
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
		assert.Equal(1, s3.Datafiles)
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

func TestGetErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("ReadError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		err = db.Put([]byte("foo"), []byte("bar"))
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("FileID").Return(0)
		mockDatafile.On("ReadAt", int64(0), int64(22)).Return(
			internal.Entry{},
			ErrMockError,
		)
		db.curr = mockDatafile

		_, err = db.Get([]byte("foo"))
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

	t.Run("ChecksumError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		err = db.Put([]byte("foo"), []byte("bar"))
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("FileID").Return(0)
		mockDatafile.On("ReadAt", int64(0), int64(22)).Return(
			internal.Entry{
				Checksum: 0x0,
				Key:      []byte("foo"),
				Offset:   0,
				Value:    []byte("bar"),
			},
			nil,
		)
		db.curr = mockDatafile

		_, err = db.Get([]byte("foo"))
		assert.Error(err)
		assert.Equal(ErrChecksumFailed, err)
	})

}

func TestPutErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("WriteError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)

		db, err := Open(testdir)
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("Size").Return(int64(0))
		mockDatafile.On(
			"Write",
			internal.Entry{
				Checksum: 0x76ff8caa,
				Key:      []byte("foo"),
				Offset:   0,
				Value:    []byte("bar"),
			},
		).Return(int64(0), int64(0), ErrMockError)
		db.curr = mockDatafile

		err = db.Put([]byte("foo"), []byte("bar"))
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

	t.Run("SyncError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		db, err := Open(testdir, WithSync(true))
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("Size").Return(int64(0))
		mockDatafile.On(
			"Write",
			internal.Entry{
				Checksum: 0x78240498,
				Key:      []byte("bar"),
				Offset:   0,
				Value:    []byte("baz"),
			},
		).Return(int64(0), int64(0), nil)
		mockDatafile.On("Sync").Return(ErrMockError)
		db.curr = mockDatafile

		err = db.Put([]byte("bar"), []byte("baz"))
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})
}

func TestOpenErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("BadPath", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		assert.NoError(ioutil.WriteFile(filepath.Join(testdir, "foo"), []byte("foo"), 0600))

		_, err = Open(filepath.Join(testdir, "foo", "tmp.db"))
		assert.Error(err)
	})

	t.Run("BadOption", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		withBogusOption := func() Option {
			return func(cfg *config.Config) error {
				return errors.New("mocked error")
			}
		}

		_, err = Open(testdir, withBogusOption())
		assert.Error(err)
	})

	t.Run("LoadDatafilesError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir)
		assert.NoError(err)

		err = db.Put([]byte("foo"), []byte("bar"))
		assert.NoError(err)

		err = db.Close()
		assert.NoError(err)

		// Simulate some horrible that happened to the datafiles!
		err = os.Rename(filepath.Join(testdir, "000000000.data"), filepath.Join(testdir, "000000000xxx.data"))
		assert.NoError(err)

		_, err = Open(testdir)
		assert.Error(err)
		assert.Equal("strconv.ParseInt: parsing \"000000000xxx\": invalid syntax", err.Error())
	})
}

func TestCloseErrors(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)
	defer os.RemoveAll(testdir)

	t.Run("CloseIndexError", func(t *testing.T) {
		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		mockIndexer := new(mocks.Indexer)
		mockIndexer.On("Save", db.trie, filepath.Join(db.path, "index")).Return(ErrMockError)
		db.indexer = mockIndexer

		err = db.Close()
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

	t.Run("CloseDatafilesError", func(t *testing.T) {
		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("Close").Return(ErrMockError)
		db.datafiles[0] = mockDatafile

		err = db.Close()
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

	t.Run("CloseActiveDatafileError", func(t *testing.T) {
		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("Close").Return(ErrMockError)
		db.curr = mockDatafile

		err = db.Close()
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})
}

func TestDeleteErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("WriteError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir, WithMaxDatafileSize(32))
		assert.NoError(err)

		err = db.Put([]byte("foo"), []byte("bar"))
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("Size").Return(int64(0))
		mockDatafile.On(
			"Write",
			internal.Entry{
				Checksum: 0x0,
				Key:      []byte("foo"),
				Offset:   0,
				Value:    []byte{},
			},
		).Return(int64(0), int64(0), ErrMockError)
		db.curr = mockDatafile

		err = db.Delete([]byte("foo"))
		assert.Error(err)
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

	t.Run("EmptyCloseError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir)
		assert.NoError(err)

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("Close").Return(ErrMockError)
		db.curr = mockDatafile

		err = db.Merge()
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

	t.Run("ReadError", func(t *testing.T) {
		testdir, err := ioutil.TempDir("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir)
		assert.NoError(err)

		assert.NoError(db.Put([]byte("foo"), []byte("bar")))

		mockDatafile := new(mocks.Datafile)
		mockDatafile.On("FileID").Return(0)
		mockDatafile.On("ReadAt", int64(0), int64(22)).Return(
			internal.Entry{},
			ErrMockError,
		)
		db.curr = mockDatafile

		err = db.Merge()
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

}

func TestConcurrent(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})
	})

	t.Run("Concurrent", func(t *testing.T) {
		t.Run("Put", func(t *testing.T) {
			f := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						key := []byte(fmt.Sprintf("k%d", i))
						value := []byte(fmt.Sprintf("v%d", i))
						err := db.Put(key, value)
						assert.NoError(err)
					}
				}
			}

			wg := &sync.WaitGroup{}

			go f(wg, 2)
			go f(wg, 3)
			go f(wg, 5)
			wg.Add(3)

			wg.Wait()
		})

		t.Run("Get", func(t *testing.T) {
			f := func(wg *sync.WaitGroup, N int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= N; i++ {
					value, err := db.Get([]byte("foo"))
					assert.NoError(err)
					assert.Equal([]byte("bar"), value)
				}
			}

			wg := &sync.WaitGroup{}

			go f(wg, 100)
			go f(wg, 100)
			go f(wg, 100)
			wg.Add(3)

			wg.Wait()
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestScan(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("Put", func(t *testing.T) {
			var items = map[string][]byte{
				"1":     []byte("1"),
				"2":     []byte("2"),
				"3":     []byte("3"),
				"food":  []byte("pizza"),
				"foo":   []byte([]byte("foo")),
				"fooz":  []byte("fooz ball"),
				"hello": []byte("world"),
			}
			for k, v := range items {
				err = db.Put([]byte(k), v)
				assert.NoError(err)
			}
		})
	})

	t.Run("Scan", func(t *testing.T) {
		var (
			vals     [][]byte
			expected = [][]byte{
				[]byte("foo"),
				[]byte("fooz ball"),
				[]byte("pizza"),
			}
		)

		err = db.Scan([]byte("fo"), func(key []byte) error {
			val, err := db.Get(key)
			assert.NoError(err)
			vals = append(vals, val)
			return nil
		})
		vals = SortByteArrays(vals)
		assert.Equal(expected, vals)
	})

	t.Run("ScanErrors", func(t *testing.T) {
		err = db.Scan([]byte("fo"), func(key []byte) error {
			return ErrMockError
		})
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})
}

func TestLocking(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	db, err := Open(testdir)
	assert.NoError(err)
	defer db.Close()

	_, err = Open(testdir)
	assert.Error(err)
	assert.Equal(ErrDatabaseLocked, err)
}

type benchmarkTestCase struct {
	name string
	size int
}

func BenchmarkGet(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	testdir, err := ioutil.TempDir(currentDir, "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))

			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))

			options := []Option{
				WithMaxKeySize(uint32(len(key))),
				WithMaxValueSize(uint64(tt.size)),
			}
			db, err := Open(testdir, options...)
			if err != nil {
				b.Fatal(err)
			}

			err = db.Put(key, value)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(val, value) {
					b.Errorf("unexpected value")
				}
			}
			b.StopTimer()
			db.Close()
		})
	}
}

func BenchmarkPut(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			WithSync(false),
		},
		"Sync": {
			WithSync(true),
		},
	}

	for name, options := range variants {
		testdir, err := ioutil.TempDir(currentDir, "bitcask_bench")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(testdir)

		db, err := Open(testdir, options...)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		for _, tt := range tests {
			b.Run(tt.name+name, func(b *testing.B) {
				b.SetBytes(int64(tt.size))

				key := []byte("foo")
				value := []byte(strings.Repeat(" ", tt.size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					err := db.Put(key, value)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkScan(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	testdir, err := ioutil.TempDir(currentDir, "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)

	db, err := Open(testdir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var items = map[string][]byte{
		"1":     []byte("1"),
		"2":     []byte("2"),
		"3":     []byte("3"),
		"food":  []byte("pizza"),
		"foo":   []byte([]byte("foo")),
		"fooz":  []byte("fooz ball"),
		"hello": []byte("world"),
	}
	for k, v := range items {
		err := db.Put([]byte(k), v)
		if err != nil {
			b.Fatal(err)
		}
	}

	var expected = [][]byte{[]byte("foo"), []byte("food"), []byte("fooz")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var keys [][]byte
		err = db.Scan([]byte("fo"), func(key []byte) error {
			keys = append(keys, key)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		keys = SortByteArrays(keys)
		if !reflect.DeepEqual(expected, keys) {
			b.Fatal(fmt.Errorf("expected keys=#%v got=%#v", expected, keys))
		}
	}
}
