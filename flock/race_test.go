package flock

// the "nd" in "nd_test.go" stands for non-deterministic

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The two tests in this file are test some concurrency scenarios :
// 1- TestRaceLock() runs several threads racing for the same lock
// 2- TestShatteredLock() runs racing racing threads, along with another threads which forcibly remove the file from disk
//
// Note that these tests are non-deterministic : the coverage produced by each test depends
// on how the runtime chooses to schedule the concurrent goroutines.

var lockerCount int64

// lockAndCount tries to take a lock on "lockpath"
// if it fails : it returns 0 and stop there
// if it succeeds :
//   1- it sets a defer function to release the lock in the same fashion as "func (b *Bitcask) Close()"
//   2- it increments the shared "lockerCount" above
//   3- it waits for a short amount of time
//   4- it decrements "lockerCount"
//   5- it returns the value it has seen at step 2.
//
// If the locking and unlocking behave as we expect them to,
// instructions 1-5 should be in a critical section,
// and the only possible value at step 2 should be "1".
//
// Returning a value > 0 indicates this function successfully acquired the lock,
// returning a value == 0 indicates that TryLock failed.

func lockAndCount(lockpath string) int64 {
	lock := New(lockpath)
	ok, _ := lock.TryLock()
	if !ok {
		return 0
	}
	defer func() {
		lock.Unlock()
	}()

	x := atomic.AddInt64(&lockerCount, 1)
	// emulate a workload :
	<-time.After(1 * time.Microsecond)
	atomic.AddInt64(&lockerCount, -1)

	return x
}

// locker will call the lock function above in a loop, until one of the following holds :
//  - reading from the "timeout" channel doesn't block
//  - the number of calls to "lock()" that indicate the lock was successfully taken reaches "successfullLockCount"
func locker(t *testing.T, id int, lockPath string, successfulLockCount int, timeout <-chan struct{}) {
	timedOut := false

	failCount := 0
	max := int64(0)

lockloop:
	for successfulLockCount > 0 {
		select {
		case <-timeout:
			timedOut = true
			break lockloop
		default:
		}

		x := lockAndCount(lockPath)

		if x > 0 {
			// if x indicates the lock was taken : decrement the counter
			successfulLockCount--
		}

		if x > 1 {
			// if x indicates an invalid value : increase the failCount and update max accordingly
			failCount++
			if x > max {
				max = x
			}
		}
	}

	// check failure cases :
	if timedOut {
		t.Fail()
		t.Logf("[runner %02d] timed out", id)
	}
	if failCount > 0 {
		t.Fail()
		t.Logf("[runner %02d] lockCounter was > 1 on %2.d occasions, max seen value was %2.d", id, failCount, max)
	}
}

// TestRaceLock checks that no error occurs when several concurrent actors (goroutines in this case) race for the same lock.
func TestRaceLock(t *testing.T) {
	// test parameters, written in code :
	//   you may want to tweak these values for testing

	goroutines := 20          // number of concurrent "locker" goroutines to launch
	successfulLockCount := 50 // how many times a "locker" will successfully take the lock before halting

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)

	// timeout implemented in code
	// (the lock acquisition depends on the behavior of the filesystem,
	//	avoid sending CI in endless loop if something fishy happens on the test server ...)
	// tweak this value if needed ; comment out the "close(ch)" instruction below
	timeout := 10 * time.Second
	ch := make(chan struct{})
	go func() {
		<-time.After(timeout)
		close(ch)
	}()

	wg := &sync.WaitGroup{}
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			locker(t, id, testLockPath, successfulLockCount, ch)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func isExpectedError(err error) bool {
	switch {
	case err == nil:
		return true
	case err == ErrInodeChangedAtPath:
		return true
	case errors.Is(err, syscall.ENOENT):
		return true

	default:
		return false
	}
}

// TestShatteredLock runs concurrent goroutines on one lock, with an extra goroutine
// which removes the lock file from disk without checking the locks
// (e.g: a user who would run 'rm lockfile' in a loop while the program is running).
//
// In this scenario, errors may occur on .Unlock() ; this test checks that only errors
// relating to the file being deleted occur.
//
// This test additionally logs the number of errors that occurred, grouped by error message.
func TestShatteredLock(t *testing.T) {
	// test parameters, written in code :
	//   you may want to tweak these values for testing

	goroutines := 4           // number of concurrent "locker" and "remover" goroutines to launch
	successfulLockCount := 10 // how many times a "locker" will successfully take the lock before halting

	// make sure there is no present lock when startng this test
	os.Remove(testLockPath)
	assert := assert.New(t)

	wg := &sync.WaitGroup{}
	wg.Add(goroutines)

	stopChan := make(chan struct{})

	errChan := make(chan error, 10)

	for i := 0; i < goroutines; i++ {
		go func(id int, count int) {
			for count > 0 {
				lock := New(testLockPath)
				ok, _ := lock.TryLock()
				if !ok {
					continue
				}

				count--
				err := lock.Unlock()
				if !isExpectedError(err) {
					assert.Fail("goroutine %d - unexpected error: %v", id, err)
				}

				if err != nil {
					errChan <- err
				}
			}

			wg.Done()
		}(i, successfulLockCount)
	}

	var wgCompanion = &sync.WaitGroup{}
	wgCompanion.Add(2)

	go func() {
		defer wgCompanion.Done()
		for {
			os.Remove(testLockPath)

			select {
			case <-stopChan:
				return
			default:
			}
		}
	}()

	var errs = make(map[string]int)
	go func() {
		for err := range errChan {
			errs[err.Error()]++
		}
		wgCompanion.Done()
	}()

	wg.Wait()
	close(stopChan)
	close(errChan)
	wgCompanion.Wait()

	for err, count := range errs {
		t.Logf("  seen %d times: %s", count, err)
	}
}
