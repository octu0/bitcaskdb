package flock

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// WARNING : this test will delete the file located at "testLockPath". Choose an adequate temporary file name.
const testLockPath = "/tmp/bitcask_unit_test_lock" // file path to use for the lock

func TestTryLock(t *testing.T) {
	// test that basic locking functionnalities are consistent

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	assert := assert.New(t)

	lock1 := New(testLockPath)
	lock2 := New(testLockPath)

	// 1- take the first lock
	locked1, err := lock1.TryLock()
	assert.True(locked1)
	assert.NoError(err)

	// 2- check that the second lock cannot acquire the lock
	locked2, err := lock2.TryLock()
	assert.False(locked2)
	assert.Error(err)

	// 3- release the first lock
	err = lock1.Unlock()
	assert.NoError(err)

	// 4- check that the second lock can acquire and then release the lock without error
	locked2, err = lock2.TryLock()
	assert.True(locked2)
	assert.NoError(err)

	err = lock2.Unlock()
	assert.NoError(err)
}

func TestLock(t *testing.T) {
	assert := assert.New(t)

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	syncChan := make(chan bool)

	// main goroutine: take lock on testPath
	lock := New(testLockPath)

	err := lock.Lock()
	assert.NoError(err)

	go func() {
		// sub routine:
		lock := New(testLockPath)

		// before entering the block '.Lock()' call, signal we are about to do it
		// see below : the main goroutine will wait for a small delay before releasing the lock
		syncChan <- true
		// '.Lock()' should ultimately return without error :
		err := lock.Lock()
		assert.NoError(err)

		err = lock.Unlock()
		assert.NoError(err)

		close(syncChan)
	}()

	// wait for the "ready" signal from the sub routine,
	<-syncChan

	// after that signal wait for a small delay before releasing the lock
	<-time.After(100 * time.Microsecond)
	err = lock.Unlock()
	assert.NoError(err)

	// wait for the sub routine to finish
	<-syncChan
}

func TestErrorConditions(t *testing.T) {
	// error conditions implemented in this version :
	// - you can't release a lock you do not hold
	// - you can't lock twice the same lock

	// -- setup
	assert := assert.New(t)

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	lock := New(testLockPath)

	// -- run tests :

	err := lock.Unlock()
	assert.Error(err, "you can't release a lock you do not hold")

	// take the lock once:
	lock.TryLock()

	locked, err := lock.TryLock()
	assert.False(locked)
	assert.Error(err, "you can't lock twice the same lock (using .TryLock())")

	err = lock.Lock()
	assert.Error(err, "you can't lock twice the same lock (using .Lock())")

	// -- teardown
	lock.Unlock()
}
