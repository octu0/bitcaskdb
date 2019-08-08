package internal

import (
	"hash/crc32"
)

// Entry represents a key/value in the database
type Entry struct {
	Checksum uint32
	Key      []byte
	Offset   int64
	Value    []byte
}

func NewEntry(key, value []byte) Entry {
	checksum := crc32.ChecksumIEEE(value)

	return Entry{
		Checksum: checksum,
		Key:      key,
		Value:    value,
	}
}
