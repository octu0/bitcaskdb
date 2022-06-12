package datafile

import (
	"github.com/pkg/errors"
)

var (
	// ErrChecksumFailed is the error returned if a key/value retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")

	errNegativeRead = errors.New("error: negative read")

	errReadonly  = errors.New("error: read only datafile")
	errReadError = errors.New("error: read error")
)
