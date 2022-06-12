package codec

import (
	"bytes"
	"testing"

	"github.com/octu0/bitcaskdb/context"
)

func TestTemporaryData(t *testing.T) {
	t.Run("default_out_emory", func(tt *testing.T) {
		data := []byte("hello world")
		temp := newTemopraryData(context.Default(), "", 0)
		temp.Write(data)

		out := bytes.NewBuffer(nil)
		temp.WriteTo(out)

		if bytes.Equal(data, out.Bytes()) != true {
			tt.Errorf("expect:%s actual:%s", data, out.Bytes())
		}
		if err := temp.Close(); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if temp.file != nil {
			tt.Errorf("no tempfile")
		}
	})
	t.Run("memory", func(tt *testing.T) {
		data := []byte("hello world")
		temp := newTemopraryData(context.Default(), "", int64(len(data)))
		temp.Write(data)

		out := bytes.NewBuffer(nil)
		temp.WriteTo(out)

		if bytes.Equal(data, out.Bytes()) != true {
			tt.Errorf("expect:%s actual:%s", data, out.Bytes())
		}
		if err := temp.Close(); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if temp.file != nil {
			tt.Errorf("no tempfile")
		}
	})
	t.Run("swap_tempfile", func(tt *testing.T) {
		data := []byte("hello world")
		temp := newTemopraryData(context.Default(), "", 1)
		temp.Write(data)

		out := bytes.NewBuffer(nil)
		temp.WriteTo(out)

		if bytes.Equal(data, out.Bytes()) != true {
			tt.Errorf("expect:%s actual:%s", data, out.Bytes())
		}
		if temp.file == nil {
			tt.Errorf("tempfile created")
		}
		if err := temp.Close(); err != nil {
			tt.Errorf("no error: %+v", err)
		}
	})
}
