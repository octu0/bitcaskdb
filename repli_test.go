package bitcaskdb

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
)

func TestBitcaskRepli(t *testing.T) {
	t.Run("emptysource_emptydest", func(tt *testing.T) {
		srcdir, err := os.MkdirTemp("", "bitcask_src")
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		dstdir, err := os.MkdirTemp("", "bitcask_dst")
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}

		srcdb, err := Open(srcdir, WithRepli("127.0.0.1", 4220))
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer srcdb.Close()

		dstdb, err := Open(dstdir, WithRepliClient("127.0.0.1", 4220))
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer dstdb.Close()

		if srcdb.Len() != dstdb.Len() {
			tt.Fatalf("initial same size")
		}

		if err := srcdb.Put([]byte("test1"), bytes.NewReader([]byte("value1"))); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		time.Sleep(100 * time.Millisecond) // wait replica

		r, err := dstdb.Get([]byte("test1"))
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		defer r.Close()

		buf := bytes.NewBuffer(nil)
		buf.ReadFrom(r)

		if bytes.Equal([]byte("value1"), buf.Bytes()) != true {
			tt.Errorf("replicate test1(inserted)")
		}

		if err := srcdb.Delete([]byte("test1")); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		time.Sleep(100 * time.Millisecond) // wait replica

		if dstdb.Has([]byte("test1")) {
			tt.Errorf("replicate test1(deleted)")
		}
	})
	t.Run("src_emptydest", func(tt *testing.T) {
		srcdir, err := os.MkdirTemp("", "bitcask_src")
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		dstdir, err := os.MkdirTemp("", "bitcask_dst")
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}

		srcdb, err := Open(srcdir, WithRepli("127.0.0.1", 4220))
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer srcdb.Close()

		if err := srcdb.Put([]byte("test1"), bytes.NewReader([]byte("value1"))); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if err := srcdb.Put([]byte("test12"), bytes.NewReader([]byte("value12"))); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if err := srcdb.Put([]byte("test123"), bytes.NewReader([]byte("value123"))); err != nil {
			tt.Fatalf("no error %+v", err)
		}

		dstdb, err := Open(dstdir, WithRepliClient("127.0.0.1", 4220))
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer dstdb.Close()

		if srcdb.Len() != dstdb.Len() {
			tt.Fatalf("initial same size")
		}

		keys := [][]byte{
			[]byte("test1"),
			[]byte("test12"),
			[]byte("test123"),
		}
		expectValues := [][]byte{
			[]byte("value1"),
			[]byte("value12"),
			[]byte("value123"),
		}
		for i, _ := range expectValues {
			r, err := dstdb.Get(keys[i])
			if err != nil {
				tt.Errorf("no error: %+v", err)
			}
			defer r.Close()

			buf := bytes.NewBuffer(nil)
			buf.ReadFrom(r)

			if bytes.Equal(expectValues[i], buf.Bytes()) != true {
				tt.Errorf("expect=%s actual=%s", expectValues[i], buf.Bytes())
			}
			if dstdb.Has(keys[i]) != true {
				tt.Errorf("replicate key!")
			}
		}

		if err := srcdb.Delete([]byte("test1")); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		time.Sleep(100 * time.Millisecond) // wait replica

		if dstdb.Has([]byte("test1")) {
			tt.Errorf("replicate test1(deleted)")
		}
	})
}

func TestBitcaskRepliReconnect(t *testing.T) {
	srcdir, err := os.MkdirTemp("", "bitcask_src")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	dstdir, err := os.MkdirTemp("", "bitcask_dst")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}

	srcdb, err := Open(srcdir, WithRepli("127.0.0.1", 4220))
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	t.Cleanup(func() {
		srcdb.Close()
	})

	dstdb, err := Open(dstdir, WithRepliClient("127.0.0.1", 4220))
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	t.Cleanup(func() {
		dstdb.Close()
	})

	t.Run("check_repli", func(tt *testing.T) {
		if err := srcdb.Put([]byte("test1"), bytes.NewReader([]byte("value1"))); err != nil {
			tt.Fatalf("no error %+v", err)
		}

		time.Sleep(100 * time.Millisecond)

		r, err := dstdb.Get([]byte("test1"))
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer r.Close()

		data, err := io.ReadAll(r)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if bytes.Equal([]byte("value1"), data) != true {
			tt.Errorf("expect:value1 actual:%s", data)
		}
	})

	t.Run("stop_repli", func(tt *testing.T) {
		// When stopped for some reason
		if err := srcdb.repliEmit.Stop(); err != nil {
			tt.Fatalf("no error %+v", err)
		}

		if err := srcdb.Put([]byte("test2"), bytes.NewReader([]byte("value2"))); err != nil {
			tt.Fatalf("no error %+v", err)
		}
	})

	t.Run("restart_repli", func(tt *testing.T) {
		// When recovery
		if err := srcdb.repliEmit.Start(srcdb.repliSource(), srcdb.opt.RepliBindIP, srcdb.opt.RepliBindPort); err != nil {
			tt.Fatalf("no error %+v", err)
		}

		// reconnect wait
		time.Sleep(1000 * time.Millisecond)

		r, err := dstdb.Get([]byte("test2"))
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer r.Close()

		data, err := io.ReadAll(r)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if bytes.Equal([]byte("value2"), data) != true {
			tt.Errorf("expect:value2 actual:%s", data)
		}
	})
}
