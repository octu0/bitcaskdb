package bitcaskdb

import (
	"github.com/pkg/errors"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyExpired is the error returned when a key is queried which has
	// already expired (due to ttl)
	ErrKeyExpired = errors.New("error: key expired")

	// ErrEmptyKey is the error returned for a value with an empty key.
	ErrEmptyKey = errors.New("error: empty key")

	// ErrDatabaseLocked is the error returned if the database is locked
	// (typically opened by another process)
	ErrDatabaseLocked = errors.New("error: database locked")

	// ErrInvalidRange is the error returned when the range scan is invalid
	ErrInvalidRange = errors.New("error: invalid range")

	// ErrMergeInProgress is the error returned if merge is called when already a merge
	// is in progress
	ErrMergeInProgress = errors.New("error: merge already in progress")
)
