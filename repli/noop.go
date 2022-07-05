package repli

import (
	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
)

var (
	_ Emitter = (*noopEmitter)(nil)
)

type noopEmitter struct {
}

func (*noopEmitter) Start(src Source, bindIP string, bindPort int) error {
	return nil
}

func (*noopEmitter) Stop() error {
	return nil
}

func (*noopEmitter) EmitInsert(filer indexer.Filer) error {
	return nil
}

func (*noopEmitter) EmitDelete(key []byte) error {
	return nil
}

func (*noopEmitter) EmitCurrentFileID(fileID datafile.FileID) error {
	return nil
}

func NewNoopEmitter() *noopEmitter {
	return new(noopEmitter)
}

type noopReciver struct {
}

func (*noopReciver) Start(dst Destination, serverIP string, serverPort int) error {
	return nil
}

func (*noopReciver) Stop() error {
	return nil
}

func NewNoopReciver() *noopReciver {
	return new(noopReciver)
}
