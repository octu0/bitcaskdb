package codec

import (
	"github.com/pkg/errors"
)

var (
	errInvalidKeyOrValueSize = errors.New("key/value size is invalid")
	errNegativeRead          = errors.New("negative count from io.Read")
	errTemporaryFileNotOpen  = errors.New("temporaty file is not open")
	errKeySizeTooLarge       = errors.New("key size too large")
)

func IsCorruptedData(err error) bool {
	if errors.Is(err, errInvalidKeyOrValueSize) {
		return true
	}
	if errors.Is(err, errNegativeRead) {
		return true
	}
	if errors.Is(err, errKeySizeTooLarge) {
		return true
	}
	return false
}
