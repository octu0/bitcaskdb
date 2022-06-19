package repli

import (
	"io"
	"time"

	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
)

type Emitter interface {
	Start(src Source, bindIP string, bindPort int) error
	Stop() error
	EmitInsert(filer indexer.Filer) error
	EmitDelete(key []byte) error
}

type Reciver interface {
	Start(dst Destination, serverIP string, serverPort int) error
	Stop() error
}

type Source interface {
	FileIds() []int32
	LastIndex(fileID int32) int64
	Header(fileID int32, index int64) (*datafile.Header, bool, error)
	Read(fileID int32, index int64, size int64) (*datafile.Entry, error)
}

type FileIDAndIndex struct {
	FileID int32
	Index  int64
}

type Destination interface {
	LastFiles() []FileIDAndIndex
	Insert(fileID int32, index int64, checksum uint32, key []byte, r io.Reader, expiry time.Time) error
	Delete(key []byte) error
}
