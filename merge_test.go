package bitcaskdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	var (
		db  *Bitcask
		err error
	)

	assert := assert.New(t)

	testdir, err := os.MkdirTemp("", "bitcask")
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
		testdir, err := os.MkdirTemp("", "bitcask")
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

	testdir, err := os.MkdirTemp("", "bitcask")
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

func TestMergeGoroutine(t *testing.T) {
	testdir, err := os.MkdirTemp("", "bitcask")
	if err != nil {
		t.Fatalf("no error! %+v", err)
	}
	db, err := Open(testdir, WithMaxDatafileSize(32))
	if err != nil {
		t.Fatalf("no error! %+v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := new(sync.WaitGroup)
	for i := 0; i < 100; i += 1 {
		wg.Add(1)
		go func(ctx context.Context, w *sync.WaitGroup, id int) {
			defer w.Done()

			j := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				j += 1

				key := []byte(fmt.Sprintf("%d-%d", id, j))
				if err := db.PutBytes(key, key); err != nil {
					t.Errorf("no error %+v", err)
				}
				if db.Has(key) != true {
					t.Errorf("%s exists!", key)
				}
				e, err := db.Get(key)
				if err != nil {
					t.Errorf("no error! %+v", err)
				}
				defer e.Close()
			}
		}(ctx, wg, i)
	}

	wg.Add(1)
	go func(ctx context.Context, w *sync.WaitGroup) {
		defer w.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			t.Logf("merge start")
			if err := db.Merge(); err != nil {
				t.Errorf("no error! %+v", err)
			}
			t.Logf("merged")
		}
	}(ctx, wg)

	<-ctx.Done()
	wg.Wait()
	db.Close()
}
