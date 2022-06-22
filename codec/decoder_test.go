package codec

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"io"
	"testing"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/runtime"
)

const (
	b64EmptyValue string = "AAAABwAAAAAAAAAAAAAAAAAAAAAAAAAAdGVzdEtleQ=="
)

func TestDecoderShortPrefix(t *testing.T) {
	t.Parallel()

	prefix := make([]byte, headerKeySize+headerValueSize)
	binary.BigEndian.PutUint32(prefix, 1)
	binary.BigEndian.PutUint64(prefix[headerKeySize:], 1)

	truncBytesCount := int64(2)
	buf := bytes.NewReader(prefix[:headerKeySize+headerValueSize-truncBytesCount])
	decoder := NewDecoder(runtime.DefaultContext(), buf)
	defer decoder.Close()

	_, err := decoder.Decode()
	if err == nil {
		t.Errorf("short size decode error")
	}
	if errors.Is(err, io.EOF) != true {
		t.Errorf("io.EOF! %+v", err)
	}
}

func TestDecoderInvalidValueKeySizes(t *testing.T) {
	tests := []struct {
		keySize   uint32
		valueSize uint64
		name      string
	}{
		{keySize: 0, valueSize: 5, name: "zero key size"}, //zero value size is correct for tombstones
	}

	for i := range tests {
		i := i
		t.Run(tests[i].name, func(tt *testing.T) {
			tt.Parallel()
			prefix := make([]byte, HeaderSize)
			binary.BigEndian.PutUint32(prefix[:headerKeySize], tests[i].keySize)
			binary.BigEndian.PutUint64(prefix[headerKeySize:headerKeySize+headerValueSize], tests[i].valueSize)

			buf := bytes.NewReader(prefix)
			decoder := NewDecoder(runtime.DefaultContext(), buf)
			defer decoder.Close()

			_, err := decoder.Decode()
			if err == nil {
				tt.Errorf("must error")
			}
			if errors.Is(err, errInvalidKeyOrValueSize) != true {
				tt.Errorf("errInvalidKeyOrValueSize! %+v", err)
			}
		})
	}
}

func TestDecoderTruncatedData(t *testing.T) {
	key := []byte("foo")
	value := []byte("bar")
	data := make([]byte, headerKeySize+headerValueSize+headerChecksumSize+int64(len(key))+int64(len(value)))

	binary.BigEndian.PutUint32(data, uint32(len(key)))
	binary.BigEndian.PutUint64(data[headerKeySize:], uint64(len(value)))
	copy(data[headerKeySize+headerValueSize:], key)
	copy(data[headerKeySize+headerValueSize+int64(len(key)):], value)
	copy(data[headerKeySize+headerValueSize+int64(len(key))+int64(len(value)):], bytes.Repeat([]byte("0"), int(headerChecksumSize)))

	tests := []struct {
		data []byte
		name string
	}{
		{data: data[:headerKeySize+headerValueSize+int64(len(key))-1], name: "truncated key"},
		{data: data[:headerKeySize+headerValueSize+int64(len(key))+int64(len(value))-1], name: "truncated value"},
		{data: data[:headerKeySize+headerValueSize+int64(len(key))+int64(len(value))+headerChecksumSize-1], name: "truncated checksum"},
	}

	for i := range tests {
		i := i
		t.Run(tests[i].name, func(tt *testing.T) {
			tt.Parallel()
			buf := bytes.NewReader(tests[i].data)
			decoder := NewDecoder(runtime.DefaultContext(), buf)
			defer decoder.Close()

			_, err := decoder.Decode()
			if errors.Is(err, io.EOF) != true {
				tt.Errorf("io.EOF!: %+v", err)
			}
		})
	}
}

func TestDecoderPayload(t *testing.T) {
	t.Parallel()

	data := make([]byte, HeaderSize+2)
	binary.BigEndian.PutUint32(data[0:headerKeySize], 1)
	binary.BigEndian.PutUint64(data[headerKeySize:headerKeySize+headerValueSize], 2)
	binary.BigEndian.PutUint32(data[headerKeySize+headerValueSize:headerKeySize+headerValueSize+headerChecksumSize], 3)
	binary.BigEndian.PutUint64(data[headerKeySize+headerValueSize+headerChecksumSize:headerKeySize+headerValueSize+headerChecksumSize+headerTTLSize], 4)

	copy(data[HeaderSize:HeaderSize+1], []byte("a"))
	copy(data[HeaderSize+1:HeaderSize+2], []byte("b"))

	d := NewDecoder(runtime.DefaultContext(), bytes.NewReader(data))
	defer d.Close()

	p, err := d.Decode()
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	defer p.Close()

	if bytes.Equal(p.Key, []byte("a")) != true {
		t.Errorf("key is a")
	}
	body, err := io.ReadAll(p.Value)
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	if bytes.Equal(body, []byte("b")) != true {
		t.Errorf("value is b")
	}
	if p.Checksum != 3 {
		t.Errorf("checksum is 3")
	}
	if p.Expiry.Unix() != 4 {
		t.Errorf("expiry is 4")
	}
}

func TestDecodeNoValue(t *testing.T) {
	// encoder_test#TestEncodeNoValue
	b, _ := base64.StdEncoding.DecodeString(b64EmptyValue)
	d := NewDecoder(runtime.DefaultContext(), bytes.NewReader(b))
	defer d.Close()

	p, err := d.Decode()
	if err != nil {
		t.Errorf("no error: %+v", err)
	}
	defer p.Close()

	if string(p.Key) != string("testKey") {
		t.Errorf("decodable key")
	}
	if p.ValueSize != 0 {
		t.Errorf("value size is zero")
	}
	if p.N != HeaderSize+int64(len(p.Key)) {
		t.Errorf("meta + key only")
	}
	if _, err := p.Value.Read([]byte{}); err != nil {
		t.Errorf("no error: %+v", err)
	}
}

type testDecodeIOCounterReader struct {
	r ReaderAtSeeker
	c int
}

func (t *testDecodeIOCounterReader) Seek(off int64, whence int) (int64, error) {
	t.c += 1
	return t.r.Seek(off, whence)
}

func (t *testDecodeIOCounterReader) ReadAt(p []byte, off int64) (int, error) {
	t.c += 1
	return t.r.ReadAt(p, off)
}

func TestDecoderIOCount(t *testing.T) {
	t.Run("read/<128K", func(tt *testing.T) {
		data := make([]byte, 32*1024)
		binary.BigEndian.PutUint32(data[0:headerKeySize], 256)
		binary.BigEndian.PutUint64(data[headerKeySize:headerKeySize+headerValueSize], 30*1024)
		binary.BigEndian.PutUint32(data[headerKeySize+headerValueSize:headerKeySize+headerValueSize+headerChecksumSize], 3)
		binary.BigEndian.PutUint64(data[headerKeySize+headerValueSize+headerChecksumSize:headerKeySize+headerValueSize+headerChecksumSize+headerTTLSize], 4)

		counter := &testDecodeIOCounterReader{
			r: bytes.NewReader(data),
			c: 0,
		}
		d := NewDecoder(runtime.DefaultContext(), counter)
		defer d.Close()

		p, err := d.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		defer p.Close()
		if _, err := io.ReadAll(p.Value); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if counter.c != 4 {
			tt.Errorf("bufio Read 4: %d", counter.c)
		}
	})
	t.Run("read/128K*2", func(tt *testing.T) {
		data := make([]byte, (128*1024)*2)
		binary.BigEndian.PutUint32(data[0:headerKeySize], 256)
		binary.BigEndian.PutUint64(data[headerKeySize:headerKeySize+headerValueSize], 250*1024)
		binary.BigEndian.PutUint32(data[headerKeySize+headerValueSize:headerKeySize+headerValueSize+headerChecksumSize], 3)
		binary.BigEndian.PutUint64(data[headerKeySize+headerValueSize+headerChecksumSize:headerKeySize+headerValueSize+headerChecksumSize+headerTTLSize], 4)

		counter := &testDecodeIOCounterReader{
			r: bytes.NewReader(data),
			c: 0,
		}
		d := NewDecoder(runtime.DefaultContext(), counter)
		defer d.Close()

		p, err := d.Decode()
		if err != nil {
			tt.Errorf("no error: %+v", err)
		}
		defer p.Close()
		if _, err := io.ReadAll(p.Value); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if counter.c != 5 {
			tt.Errorf("bufio Read 5: %d", counter.c)
		}
	})
}
