package datafile

import (
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestOpen(t *testing.T) {
	t.Run("emptydir", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "test*")
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer os.RemoveAll(dir)

		df, err := Open(NextFileID(), dir)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if df.w == nil {
			tt.Errorf("writable")
		}
		if df.ra != nil {
			tt.Errorf("not readonly")
		}
	})
	t.Run("readonly", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "test*")
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer os.RemoveAll(dir)

		id := NextFileID()
		dfWrite, err := Open(id, dir)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		dfRead, err := OpenReadonly(id, dir)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if _, _, err := dfWrite.Write([]byte("test"), nil, time.Time{}); err != nil {
			tt.Errorf("writable no error %+v", err)
		}
		if _, _, err := dfRead.Write([]byte("test"), nil, time.Time{}); err != nil {
			if errors.Is(err, errReadonly) != true {
				tt.Errorf("errReadonly! %+v", err)
			}
		} else {
			tt.Errorf("must errReadonly!")
		}
	})
}
