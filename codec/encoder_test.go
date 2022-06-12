package codec

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/octu0/bitcaskdb/context"
)

func TestEncode(t *testing.T) {
	t.Parallel()

	key := []byte("mykey")
	value := []byte("myvalue")
	expiry := time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC)
	expectSize := int64(MetaInfoSize + int64(len(key)) + int64(len(value)))

	buf := bytes.NewBuffer(nil)
	encoder := NewEncoder(context.Default(), buf, "", 0)
	defer encoder.Close()

	size, err := encoder.Encode(key, bytes.NewReader(value), expiry)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if expectSize != size {
		t.Errorf("unexpected size: %d != %d", expectSize, size)
	}
	if int64(len(buf.Bytes())) != size {
		t.Errorf("written size: %d != %d", len(buf.Bytes()), size)
	}
}

func TestEncodeNoValue(t *testing.T) {
	t.Parallel()

	buf := bytes.NewBuffer(nil)
	key := []byte("testKey")
	expiry := time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC)
	encoder := NewEncoder(context.Default(), buf, "", 0)
	defer encoder.Close()

	size, err := encoder.Encode(key, nil, expiry)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if size != int64(MetaInfoSize+len(key)) {
		t.Errorf("size value is 0")
	}

	r := bytes.NewReader(buf.Bytes())
	keySize := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &keySize); err != nil {
		t.Errorf("no error: %+v", err)
	}
	valueSize := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &valueSize); err != nil {
		t.Errorf("no error: %+v", err)
	}
	checksum := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &checksum); err != nil {
		t.Errorf("no error: %+v", err)
	}
	ttl := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &ttl); err != nil {
		t.Errorf("no error: %+v", err)
	}

	if keySize != 7 {
		t.Errorf("key size is 7")
	}
	if valueSize != 0 {
		t.Errorf("value size is 0")
	}
	if checksum != 0 {
		t.Errorf("checksum is 0")
	}
	if ttl != 0 {
		t.Errorf("ttl is 0")
	}
	b := make([]byte, keySize)
	if _, err := r.Read(b); err != nil {
		t.Errorf("no error: %+v", err)
	}
	if string(key) != string(b) {
		t.Errorf("expect:%s actual:%s", key, b)
	}
	t.Logf("%s", base64.StdEncoding.EncodeToString(buf.Bytes()))
}

type testEncodeIOCounterReader struct {
	r io.Reader
	c int
}

func (t *testEncodeIOCounterReader) Read(p []byte) (int, error) {
	t.c += 1
	return t.r.Read(p)
}

type testEncodeIOCounterWriter struct {
	w io.Writer
	c int
}

func (t *testEncodeIOCounterWriter) Write(p []byte) (int, error) {
	t.c += 1
	return t.w.Write(p)
}

func TestEncodeIOCount(t *testing.T) {
	t.Run("read/<128K", func(tt *testing.T) {
		expiry := time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC)
		key := []byte("mykey")
		value := bytes.Repeat([]byte("a"), 32*1024)

		rr := &testEncodeIOCounterReader{
			r: bytes.NewReader(value),
			c: 0,
		}
		ww := &testEncodeIOCounterWriter{
			w: bytes.NewBuffer(nil),
			c: 0,
		}
		e := NewEncoder(context.Default(), ww, "", 0)
		defer e.Close()

		if _, err := e.Encode(key, rr, expiry); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if rr.c != 2 {
			tt.Errorf("read = 2 actual=%d", rr.c)
		}
		if ww.c != 1 {
			tt.Errorf("write = 1 actual=%d", ww.c)
		}
	})
	t.Run("read/128K*2", func(tt *testing.T) {
		expiry := time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC)
		key := []byte("mykey")
		value := bytes.Repeat([]byte("a"), (128*1024)*2)

		rr := &testEncodeIOCounterReader{
			r: bytes.NewReader(value),
			c: 0,
		}
		ww := &testEncodeIOCounterWriter{
			w: bytes.NewBuffer(nil),
			c: 0,
		}
		e := NewEncoder(context.Default(), ww, "", 0)
		defer e.Close()

		if _, err := e.Encode(key, rr, expiry); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if rr.c != 3 {
			tt.Errorf("read = 3 actual=%d", rr.c)
		}
		if ww.c != 2 {
			tt.Errorf("write = 2 actual=%d", ww.c)
		}
	})
}
