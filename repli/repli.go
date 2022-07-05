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
	EmitCurrentFileID(datafile.FileID) error
}

type Reciver interface {
	Start(dst Destination, serverIP string, serverPort int) error
	Stop() error
}

type Source interface {
	CurrentFileID() datafile.FileID
	FileIds() []datafile.FileID
	LastIndex(datafile.FileID) int64
	Header(fileID datafile.FileID, index int64) (*datafile.Header, datafile.EOFType, error)
	Read(fileID datafile.FileID, index int64, size int64) (*datafile.Entry, error)
}

type FileIDAndIndex struct {
	FileID datafile.FileID
	Index  int64
}

type Destination interface {
	SetCurrentFileID(datafile.FileID) error
	LastFiles() []FileIDAndIndex
	Insert(fileID datafile.FileID, index int64, checksum uint32, key []byte, r io.Reader, expiry time.Time) error
	Delete(key []byte) error
}
