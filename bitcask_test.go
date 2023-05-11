package bitcaskdb

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	ErrMockError = errors.New("error: mock error")
)

func ExampleBasics() {
	db, err := Open("./data/mydb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// PutBytes() can be set using byte slice
	db.PutBytes([]byte("hello"), []byte("world"))

	// Get() returns io.ReadCloser
	r, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", data)

	// Put() can be specify io.Reader
	db.Put([]byte("foo"), bytes.NewReader([]byte("very large data...")))

	// PutWithTTL/PutBytesWithTTL can be set to data with expiration time
	db.PutWithTTL([]byte("bar"), bytes.NewReader(data), 10*time.Second)

	// flushes all buffers to disk
	db.Sync()

	foo, err := db.Get([]byte("foo"))
	if err != nil {
		panic(err)
	}
	defer foo.Close()

	head := make([]byte, 5)
	foo.Read(head)
	fmt.Printf("%s\n", head[0:4])
	foo.Read(head)
	fmt.Printf("%s\n", head[0:4])

	// Delete() can delete data with key
	db.Delete([]byte("foo"))

	// deletes all expired keys
	db.RunGC()

	// Merge() rebuilds databases and reclaims disk space
	db.Merge()

	// Output:
	// world
	// very
	// larg
}

func TestAll(t *testing.T) {
	var (
		db      *Bitcask
		testdir string
		err     error
	)

	assert := assert.New(t)

	testdir, err = os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	db, err = Open(testdir)
	if err != nil {
		t.Fatalf("open err: %+v", err)
	}

	t.Run("Put", func(t *testing.T) {
		err = db.PutBytes([]byte("foo"), []byte("bar"))
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		val, err := db.Get([]byte("foo"))
		assert.NoError(err)
		defer val.Close()
		data, err := io.ReadAll(val)
		assert.NoError(err)

		assert.Equal([]byte("bar"), data)
	})

	t.Run("Len", func(t *testing.T) {
		assert.Equal(1, db.Len())
	})

	t.Run("PutWithTTL", func(t *testing.T) {
		err = db.PutBytesWithTTL([]byte("bar"), []byte("baz"), 0)
		assert.NoError(err)
	})

	t.Run("GetExpiredKey", func(t *testing.T) {
		time.Sleep(time.Millisecond)
		_, err := db.Get([]byte("bar"))
		assert.Error(err)
		assert.Equal(ErrKeyExpired, err)
	})

	t.Run("Has", func(t *testing.T) {
		assert.True(db.Has([]byte("foo")))
	})

	t.Run("HasWithExpired", func(t *testing.T) {
		err = db.PutBytesWithTTL([]byte("bar"), []byte("baz"), 0)
		assert.NoError(err)
		time.Sleep(time.Millisecond)
		assert.False(db.Has([]byte("bar")))
	})

	t.Run("RunGC", func(t *testing.T) {
		err = db.PutBytesWithTTL([]byte("bar"), []byte("baz"), 0)
		assert.NoError(err)
		time.Sleep(time.Millisecond)
		err = db.RunGC()
		assert.NoError(err)
		_, err := db.Get([]byte("bar"))
		assert.Error(err)
		assert.Equal(ErrKeyNotFound, err)
	})

	t.Run("Keys", func(t *testing.T) {
		keys := make([][]byte, 0)
		for key := range db.Keys() {
			keys = append(keys, key)
		}
		assert.Equal([][]byte{[]byte("foo")}, keys)
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

	t.Run("DeleteAll", func(t *testing.T) {
		err = db.DeleteAll()
		assert.NoError(err)
	})

	t.Run("Close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(err)
	})
}

func TestDeleteAll(t *testing.T) {
	assert := assert.New(t)
	testdir, _ := os.MkdirTemp("", "bitcask")
	db, _ := Open(testdir)
	_ = db.PutBytes([]byte("foo"), []byte("foo"))
	_ = db.PutBytes([]byte("bar"), []byte("bar"))
	_ = db.PutBytes([]byte("baz"), []byte("baz"))
	assert.Equal(3, db.Len())
	err := db.DeleteAll()
	assert.NoError(err)
	assert.Equal(0, db.Len())
	_, err = db.Get([]byte("foo"))
	assert.Equal(ErrKeyNotFound, err)
	_, err = db.Get([]byte("bar"))
	assert.Equal(ErrKeyNotFound, err)
	_, err = db.Get([]byte("baz"))
	assert.Equal(ErrKeyNotFound, err)
}

func TestReopen1(t *testing.T) {
	assert := assert.New(t)
	for i := 0; i < 10; i++ {
		testdir, _ := os.MkdirTemp("", "bitcask")
		db, _ := Open(testdir, WithMaxDatafileSize(1))
		_ = db.PutBytes([]byte("foo"), []byte("bar"))
		_ = db.PutBytes([]byte("foo"), []byte("bar1"))
		_ = db.PutBytes([]byte("foo"), []byte("bar2"))
		_ = db.PutBytes([]byte("foo"), []byte("bar3"))
		_ = db.PutBytes([]byte("foo"), []byte("bar4"))
		_ = db.PutBytes([]byte("foo"), []byte("bar5"))
		if err := db.Reopen(); err != nil {
			t.Errorf("no error! %+v", err)
		}
		val, err := db.Get([]byte("foo"))
		if err != nil {
			t.Fatalf("no error! %+v", err)
		}
		defer val.Close()
		data, _ := io.ReadAll(val)
		assert.Equal("bar5", string(data))
	}
}

func TestReopen(t *testing.T) {
	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
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

		t.Run("PutBytes", func(t *testing.T) {
			err = db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("PutBytesWithTTL", func(t *testing.T) {
			err = db.PutBytesWithTTL([]byte("bar"), []byte("baz"), 0)
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err2 := io.ReadAll(val)
			assert.NoError(err2)
			assert.Equal([]byte("bar"), data)
		})

		t.Run("Reopen", func(t *testing.T) {
			err = db.Reopen()
			assert.NoError(err)
		})

		t.Run("GetAfterReopen", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err2 := io.ReadAll(val)
			assert.NoError(err2)
			assert.Equal([]byte("bar"), data)
		})

		t.Run("PutAfterReopen", func(t *testing.T) {
			err = db.PutBytes([]byte("zzz"), []byte("foo"))
			assert.NoError(err)
		})

		t.Run("GetAfterReopenAndPut", func(t *testing.T) {
			val, err := db.Get([]byte("zzz"))
			assert.NoError(err)
			defer val.Close()
			data, err2 := io.ReadAll(val)
			assert.NoError(err2)
			assert.Equal([]byte("foo"), data)
		})

		t.Run("GetExpiredKeyAfterReopen", func(t *testing.T) {
			val, err := db.Get([]byte("bar"))
			assert.Error(err)
			assert.Equal(ErrKeyExpired, err)
			assert.Nil(val)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestDeletedKeys(t *testing.T) {
	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
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

		t.Run("PutBytes", func(t *testing.T) {
			err = db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)

			assert.Equal([]byte("bar"), data)
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

func TestMetadata(t *testing.T) {
	assert := assert.New(t)
	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)
	defer os.RemoveAll(testdir)

	db, err := Open(testdir)
	assert.NoError(err)
	err = db.PutBytes([]byte("foo"), []byte("bar"))
	assert.NoError(err)
	err = db.Close()
	assert.NoError(err)
	db, err = Open(testdir)
	assert.NoError(err)

	t.Run("IndexUptoDateAfterCloseAndOpen", func(t *testing.T) {
		assert.Equal(true, db.metadata.IndexUpToDate)
	})
	t.Run("IndexUptoDateAfterPut", func(t *testing.T) {
		assert.NoError(db.PutBytes([]byte("foo1"), []byte("bar1")))
		assert.Equal(false, db.metadata.IndexUpToDate)
	})
	t.Run("Reclaimable", func(t *testing.T) {
		assert.Equal(int64(0), db.metadata.ReclaimableSpace)
	})
	t.Run("ReclaimableAfterNewPut", func(t *testing.T) {
		assert.NoError(db.PutBytes([]byte("hello"), []byte("world")))
		assert.Equal(int64(0), db.metadata.ReclaimableSpace)
	})
	t.Run("ReclaimableAfterRepeatedPut", func(t *testing.T) {
		assert.NoError(db.PutBytes([]byte("hello"), []byte("world")))
		assert.Equal(int64(34), db.metadata.ReclaimableSpace)
	})
	t.Run("ReclaimableAfterDelete", func(t *testing.T) {
		assert.NoError(db.Delete([]byte("hello")))
		assert.Equal(int64(68), db.metadata.ReclaimableSpace)
	})
	t.Run("ReclaimableAfterNonExistingDelete", func(t *testing.T) {
		assert.NoError(db.Delete([]byte("hello1")))
		assert.Equal(int64(68), db.metadata.ReclaimableSpace)
	})
	t.Run("ReclaimableAfterDeleteAll", func(t *testing.T) {
		assert.NoError(db.DeleteAll())
		assert.Equal(int64(130), db.metadata.ReclaimableSpace)
	})
	t.Run("ReclaimableAfterMerge", func(t *testing.T) {
		assert.NoError(db.Merge())
		assert.Equal(int64(0), db.metadata.ReclaimableSpace)
	})
	t.Run("IndexUptoDateAfterMerge", func(t *testing.T) {
		assert.Equal(true, db.metadata.IndexUpToDate)
	})
	t.Run("ReclaimableAfterMergeAndDeleteAll", func(t *testing.T) {
		assert.NoError(db.DeleteAll())
		assert.Equal(int64(0), db.metadata.ReclaimableSpace)
	})
}

func TestLoadIndexes(t *testing.T) {
	assert := assert.New(t)
	testdir, err1 := os.MkdirTemp("", "bitcask")
	assert.NoError(err1)
	defer os.RemoveAll(testdir)

	var db *Bitcask
	var err error

	t.Run("Setup", func(t *testing.T) {
		db, err = Open(testdir)
		assert.NoError(err)
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val := fmt.Sprintf("val%d", i)
			err := db.PutBytes([]byte(key), []byte(val))
			assert.NoError(err)
		}
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("foo%d", i)
			val := fmt.Sprintf("bar%d", i)
			err := db.PutBytesWithTTL([]byte(key), []byte(val), time.Duration(i)*time.Second)
			assert.NoError(err)
		}
		err = db.Close()
		assert.NoError(err)
	})

	t.Run("OpenAgain", func(t *testing.T) {
		db, err = Open(testdir)
		assert.NoError(err)
		assert.Equal(10, db.trie.Size())
		assert.Equal(5, db.ttlIndex.Size())
	})
}

func TestReIndex(t *testing.T) {
	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
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
			err = db.PutBytes([]byte("foo"), []byte("foovalue"))
			assert.NoError(err)
			err = db.PutBytes([]byte("foo2"), []byte("foo2value"))
			assert.NoError(err)
		})

		t.Run("PutWithExpiry", func(t *testing.T) {
			err = db.PutBytesWithTTL([]byte("bar"), []byte("baz"), 0)
			assert.NoError(err)
		})

		t.Run("PutWithLargeExpiry", func(t *testing.T) {
			err = db.PutBytesWithTTL([]byte("bar1"), []byte("baz1"), time.Hour)
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err2 := io.ReadAll(val)
			assert.NoError(err2)
			assert.Equal([]byte("foovalue"), data)
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
			err := os.Remove(filepath.Join(testdir, filerIndexFile))
			assert.NoError(err)
			err = os.Remove(filepath.Join(testdir, ttlIndexFile))
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
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)

			assert.Equal([]byte("foovalue"), data)
		})

		t.Run("GetKeyWithExpiry", func(t *testing.T) {
			{
				val, err := db.Get([]byte("bar"))
				assert.Error(err)
				assert.Equal(ErrKeyExpired, err)
				assert.Nil(val)
			}
			{
				val, err := db.Get([]byte("bar1"))
				assert.NoError(err)
				defer val.Close()
				data, err := io.ReadAll(val)
				assert.NoError(err)

				assert.Equal([]byte("baz1"), data)
			}
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(err)
		})
	})
}

func TestReIndexDeletedKeys(t *testing.T) {
	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
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

		t.Run("PutBytes", func(t *testing.T) {
			err = db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)

			assert.Equal([]byte("bar"), data)
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
			err := os.Remove(filepath.Join(testdir, filerIndexFile))
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

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testdir, WithSync(true))
		assert.NoError(err)
	})

	t.Run("PutBytes", func(t *testing.T) {
		key := []byte(strings.Repeat(" ", 17))
		value := []byte("foobar")
		err = db.PutBytes(key, value)
		assert.NoError(err)
	})

	t.Run("PutBytes", func(t *testing.T) {
		err = db.PutBytes([]byte("hello"), []byte("world"))
		assert.NoError(err)
	})
}

func TestStats(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
			err := db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)
			assert.Equal([]byte("bar"), data)
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

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
			err := db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})

		t.Run("Get", func(t *testing.T) {
			val, err := db.Get([]byte("foo"))
			assert.NoError(err)
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)

			assert.Equal([]byte("bar"), data)
		})

		t.Run("Stats", func(t *testing.T) {
			stats, err := db.Stats()
			assert.NoError(err)
			assert.Equal(stats.Datafiles, 0)
			assert.Equal(stats.Keys, 1)
		})
	})

	t.Run("Test", func(t *testing.T) {
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

func TestDirFileModeBeforeUmask(t *testing.T) {
	assert := assert.New(t)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Default DirFileModeBeforeUmask is 0700", func(t *testing.T) {
			testdir, err := os.MkdirTemp("", "bitcask")
			embeddedDir := filepath.Join(testdir, "cache")
			assert.NoError(err)
			defer os.RemoveAll(testdir)

			defaultTestMode := os.FileMode(0700)

			db, err := Open(embeddedDir)
			assert.NoError(err)
			defer db.Close()
			err = filepath.Walk(testdir, func(path string, info os.FileInfo, err error) error {
				// skip the root directory
				if path == testdir {
					return nil
				}
				if info.IsDir() {
					// perms for directory on disk are filtered through defaultTestMode, AND umask of user running test.
					// this means the mkdir calls can only FURTHER restrict permissions, not grant more (preventing escalatation).
					// to make this test OS agnostic, we'll skip using golang.org/x/sys/unix, inferring umask via XOR and AND NOT.

					// create anotherDir with allPerms - to infer umask
					anotherDir := filepath.Join(testdir, "temp")
					err := os.Mkdir(anotherDir, os.ModePerm)
					assert.NoError(err)
					defer os.RemoveAll(anotherDir)

					anotherStat, err := os.Stat(anotherDir)
					assert.NoError(err)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (anotherStat.Mode() & os.ModePerm)

					assert.Equal(info.Mode()&os.ModePerm, defaultTestMode&^umask)
				}
				return nil
			})
			assert.NoError(err)
		})

		t.Run("Dir FileModeBeforeUmask is set via options for all subdirectories", func(t *testing.T) {
			testdir, err := os.MkdirTemp("", "bitcask")
			embeddedDir := filepath.Join(testdir, "cache")
			assert.NoError(err)
			defer os.RemoveAll(testdir)

			testMode := os.FileMode(0713)

			db, err := Open(embeddedDir, WithDirFileModeBeforeUmask(testMode))
			assert.NoError(err)
			defer db.Close()
			err = filepath.Walk(testdir, func(path string, info os.FileInfo, err error) error {
				// skip the root directory
				if path == testdir {
					return nil
				}
				if info.IsDir() {
					// create anotherDir with allPerms - to infer umask
					anotherDir := filepath.Join(testdir, "temp")
					err := os.Mkdir(anotherDir, os.ModePerm)
					assert.NoError(err)
					defer os.RemoveAll(anotherDir)

					anotherStat, _ := os.Stat(anotherDir)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (anotherStat.Mode() & os.ModePerm)

					assert.Equal(info.Mode()&os.ModePerm, testMode&^umask)
				}
				return nil
			})
			assert.NoError(err)
		})

	})
}

func TestFileFileModeBeforeUmask(t *testing.T) {
	assert := assert.New(t)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Default File FileModeBeforeUmask is 0600", func(t *testing.T) {
			testdir, err := os.MkdirTemp("", "bitcask")
			assert.NoError(err)
			defer os.RemoveAll(testdir)

			defaultTestMode := os.FileMode(0600)

			db, err := Open(testdir)
			assert.NoError(err)
			defer db.Close()
			err = filepath.Walk(testdir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					// the lock file is set within Flock, so ignore it
					if filepath.Base(path) == "lock" {
						return nil
					}
					// create aFile with allPerms - to infer umask
					aFilePath := filepath.Join(testdir, "temp")
					_, err := os.OpenFile(aFilePath, os.O_CREATE, os.ModePerm)
					assert.NoError(err)
					defer os.RemoveAll(aFilePath)

					fileStat, _ := os.Stat(aFilePath)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (fileStat.Mode() & os.ModePerm)

					assert.Equal(info.Mode()&os.ModePerm, defaultTestMode&^umask)
				}
				return nil
			})
			assert.NoError(err)
		})

		t.Run("File FileModeBeforeUmask is set via options for all files", func(t *testing.T) {
			testdir, err := os.MkdirTemp("", "bitcask")
			assert.NoError(err)
			defer os.RemoveAll(testdir)

			testMode := os.FileMode(0673)

			db, err := Open(testdir, WithFileFileModeBeforeUmask(testMode))
			assert.NoError(err)
			defer db.Close()
			err = filepath.Walk(testdir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					// the lock file is set within Flock, so ignore it
					if filepath.Base(path) == "lock" {
						return nil
					}
					// create aFile with allPerms - to infer umask
					aFilePath := filepath.Join(testdir, "temp")
					_, err := os.OpenFile(aFilePath, os.O_CREATE, os.ModePerm)
					assert.NoError(err)
					defer os.RemoveAll(aFilePath)

					fileStat, _ := os.Stat(aFilePath)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (fileStat.Mode() & os.ModePerm)

					assert.Equal(info.Mode()&os.ModePerm, testMode&^umask)
				}
				return nil
			})
			assert.NoError(err)
		})
	})
}

func TestMaxDatafileSize(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)
	defer os.RemoveAll(testdir)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir, WithMaxDatafileSize(32))
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
			err := db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})
	})

	t.Run("PutBytes", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := db.PutBytes([]byte(fmt.Sprintf("key_%d", i)), []byte("bar"))
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
		defer val.Close()
		data, err := io.ReadAll(val)
		assert.NoError(err)
		assert.Equal([]byte("bar"), data)

		for i := 0; i < 10; i++ {
			val, err = db.Get([]byte(fmt.Sprintf("key_%d", i)))
			assert.NoError(err)
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)
			assert.Equal([]byte("bar"), data)
		}
	})

	t.Run("Close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(err)
	})
}

func TestPutBorderCases(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyValue", func(t *testing.T) {
		testdir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(err)

		db, err := Open(testdir)
		assert.NoError(err)

		err = db.PutBytes([]byte("alice"), nil)
		assert.NoError(err)
		z, err := db.Get([]byte("alice"))
		assert.NoError(err)
		defer z.Close()
		data, err := io.ReadAll(z)
		assert.NoError(err)

		assert.Empty(data)
	})
}

func TestPutErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyKey", func(t *testing.T) {
		testdir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(err)

		db, err := Open(testdir)
		assert.NoError(err)
		err = db.PutBytes(nil, []byte("hello"))
		assert.Equal(ErrEmptyKey, err)

	})
}

func TestOpenErrors(t *testing.T) {
	assert := assert.New(t)

	t.Run("BadPath", func(t *testing.T) {
		testdir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		assert.NoError(os.WriteFile(filepath.Join(testdir, "foo"), []byte("foo"), 0600))

		_, err = Open(filepath.Join(testdir, "foo", "tmp.db"))
		assert.Error(err)
	})

	t.Run("BadOption", func(t *testing.T) {
		testdir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		withBogusOption := func() OptionFunc {
			return func(opt *option) error {
				return errors.New("mocked error")
			}
		}

		_, err = Open(testdir, withBogusOption())
		assert.Error(err)
	})

	t.Run("LoadDatafiles", func(t *testing.T) {
		testdir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(err)
		defer os.RemoveAll(testdir)

		db, err := Open(testdir)
		assert.NoError(err)

		err = db.PutBytes([]byte("foo"), []byte("bar"))
		assert.NoError(err)

		err = db.Close()
		assert.NoError(err)

		// Simulate some horrible that happened to the datafiles!
		err = os.Rename(filepath.Join(testdir, db.curr.FileID().String()+".data"), filepath.Join(testdir, db.curr.FileID().String()+"xxx.data"))
		assert.NoError(err)

		db2, err := Open(testdir)
		if err != nil {
			t.Errorf("no error! %+v", err)
		}
		if db.curr.FileID().Newer(db2.curr.FileID()) != true {
			t.Errorf("open new file: old:%s new:%s", db.curr.FileID(), db2.curr.FileID())
		}
	})
}

func TestConcurrent(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
			err = db.PutBytes([]byte("foo"), []byte("bar"))
			assert.NoError(err)
		})
	})

	t.Run("Concurrent", func(t *testing.T) {
		t.Run("PutBytes", func(t *testing.T) {
			f := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						key := []byte(fmt.Sprintf("k%d", i))
						value := []byte(fmt.Sprintf("v%d", i))
						err := db.PutBytes(key, value)
						assert.NoError(err)
					}
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(3)

			go f(wg, 2)
			go f(wg, 3)
			go f(wg, 5)

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
					defer value.Close()
					data, err := io.ReadAll(value)
					assert.NoError(err)
					assert.Equal([]byte("bar"), data)
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(3)
			go f(wg, 100)
			go f(wg, 100)
			go f(wg, 100)

			wg.Wait()
		})

		// Test concurrent Put() with concurrent Scan()
		t.Run("PutScan", func(t *testing.T) {
			doPut := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						key := []byte(fmt.Sprintf("k%d", i))
						value := []byte(fmt.Sprintf("v%d", i))
						err := db.PutBytes(key, value)
						assert.NoError(err)
					}
				}
			}

			doScan := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						err := db.Scan([]byte("k"), func(key []byte) error {
							return nil
						})
						assert.NoError(err)
					}
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(6)

			go doPut(wg, 2)
			go doPut(wg, 3)
			go doPut(wg, 5)
			go doScan(wg, 1)
			go doScan(wg, 2)
			go doScan(wg, 4)

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

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
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
				err = db.PutBytes([]byte(k), v)
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
			defer val.Close()
			data, err2 := io.ReadAll(val)
			assert.NoError(err2)

			vals = append(vals, data)
			return nil
		})
		sort.Slice(vals, func(i, j int) bool {
			switch bytes.Compare(vals[i], vals[j]) {
			case -1:
				return true
			case 0, 1:
				return false
			}
			return false
		})
		assert.Equal(expected, vals)
	})

	t.Run("ScanErrors", func(t *testing.T) {
		err = db.Scan([]byte("fo"), func(key []byte) error {
			return ErrMockError
		})
		assert.Error(err)
		if errors.Is(err, ErrMockError) != true {
			t.Errorf("resolv my error: ErrMockErr")
		}
	})
}

func TestRange(t *testing.T) {
	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	var db *Bitcask

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
		})

		t.Run("PutBytes", func(t *testing.T) {
			for i := 1; i < 10; i++ {
				key := []byte(fmt.Sprintf("foo_%d", i))
				val := []byte(fmt.Sprintf("%d", i))
				err = db.PutBytes(key, val)
				assert.NoError(err)
			}
		})
	})

	t.Run("Range", func(t *testing.T) {
		var (
			vals     [][]byte
			expected = [][]byte{
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("7"),
			}
		)

		err = db.Range([]byte("foo_3"), []byte("foo_7"), func(key []byte) error {
			val, err := db.Get(key)
			assert.NoError(err)
			defer val.Close()
			data, err := io.ReadAll(val)
			assert.NoError(err)
			vals = append(vals, data)
			return nil
		})
		sort.Slice(vals, func(i, j int) bool {
			switch bytes.Compare(vals[i], vals[j]) {
			case -1:
				return true
			case 0, 1:
				return false
			}
			return false
		})
		assert.Equal(expected, vals)
	})

	t.Run("RangeErrors", func(t *testing.T) {
		err = db.Range([]byte("foo_3"), []byte("foo_7"), func(key []byte) error {
			return ErrMockError
		})
		assert.Error(err)
		assert.Equal(ErrMockError, err)
	})

	t.Run("InvalidRange", func(t *testing.T) {
		err = db.Range([]byte("foo_3"), []byte("foo_1"), func(key []byte) error {
			return nil
		})
		assert.Error(err)
		assert.Equal(ErrInvalidRange, err)
	})
}

func TestLocking(t *testing.T) {
	testdir, err := os.MkdirTemp("", "bitcask")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}

	db, err := Open(testdir)
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	defer db.Close()

	if _, err = Open(testdir); err == nil {
		t.Errorf("must error")
	}
}

func TestRunGCDeletesAllExpired(t *testing.T) {
	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(err)

	db, err := Open(testdir)
	assert.NoError(err)
	defer db.Close()

	// Add a node to the tree that won't expire
	db.PutBytes([]byte("static"), []byte("static"))

	// Add a node that expires almost immediately to the tree
	db.PutBytesWithTTL([]byte("shortLived"), []byte("shortLived"), 0)
	db.PutBytesWithTTL([]byte("longLived"), []byte("longLived"), time.Hour)
	db.PutBytesWithTTL([]byte("longLived2"), []byte("longLived2"), time.Hour)
	db.PutBytesWithTTL([]byte("shortLived2"), []byte("shortLived2"), 0)
	db.PutBytesWithTTL([]byte("shortLived3"), []byte("shortLived3"), 0)
	db.PutBytes([]byte("static2"), []byte("static2"))

	// Sleep a bit and run the Garbage Collector
	time.Sleep(3 * time.Millisecond)
	db.RunGC()

	keys := make([][]byte, 0)
	for k := range db.Keys() {
		keys = append(keys, k)
	}

	if len(keys) != 4 {
		t.Errorf("exists items 4 actual=%d", len(keys))
	}
}

func TestReadAfterMerge(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask")
	if err != nil {
		t.Fatalf("no error: %+v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir, WithMaxDatafileSize(4*1024))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	defer db.Close()

	if err := db.PutBytes([]byte("foo"), bytes.Repeat([]byte("1234567890"), 1024)); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := db.PutBytes([]byte("bar"), bytes.Repeat([]byte("@"), 1024*1024)); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if err := db.PutBytes([]byte("baz"), bytes.Repeat([]byte("0123456789test"), 1024*1024)); err != nil {
		t.Errorf("no error: %+v", err)
	}

	e1, err := db.Get([]byte("foo"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	defer e1.Close()

	e2, err := db.Get([]byte("bar"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	defer e2.Close()

	e3, err := db.Get([]byte("baz"))
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	defer e3.Close()

	if err := db.Merge(); err != nil {
		t.Errorf("no error: %+v", err)
	}

	data1, err := io.ReadAll(e1)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if len(data1) != 10240 {
		t.Errorf("data vanished after merge: size=%d", len(data1))
	}

	data2, err := io.ReadAll(e2)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if len(data2) != 1048576 {
		t.Errorf("data vanished after merge: size=%d", len(data2))
	}

	data3, err := io.ReadAll(e3)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if len(data3) != 14680064 {
		t.Errorf("data vanished after merge: size=%d", len(data3))
	}
}
