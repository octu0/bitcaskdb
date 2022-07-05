package indexer

import (
	"bytes"
	"testing"

	"github.com/octu0/bitcaskdb/runtime"
)

func TestKeyWriteReadBytes(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	if err := writeKeyBytes(runtime.DefaultContext(), buf, []byte("/path/to/key")); err != nil {
		t.Errorf("no error: %+v", err)
	}
	data, release, err := readKeyBytes(runtime.DefaultContext(), buf)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if release == nil {
		t.Errorf("releaseFunc not nil")
	}
	defer release()

	if bytes.Equal([]byte("/path/to/key"), data) != true {
		t.Errorf("expect=%s actual=%s", []byte("/path/to/key"), data)
	}
}
