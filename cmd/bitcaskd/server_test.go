package main

import (
	"net"
	"strconv"
	"testing"

	"github.com/tidwall/redcon"
)

func TestHandleKeys(t *testing.T) {
	s, err := newServer(":61234", "./test.db")
	if err != nil {
		t.Fatalf("Unable to create server: %v", err)
	}
	s.db.Put([]byte("foo"), []byte("bar"))
	testCases := []TestCase{
		{
			Command: redcon.Command{
				Raw:  []byte("KEYS *"),
				Args: [][]byte{[]byte("KEYS"), []byte("*")},
			},
			Expected: "1,foo",
		},
		{
			Command: redcon.Command{
				Raw:  []byte("KEYS fo*"),
				Args: [][]byte{[]byte("KEYS"), []byte("fo*")},
			},
			Expected: "1,foo",
		},
		{
			Command: redcon.Command{
				Raw:  []byte("KEYS ba*"),
				Args: [][]byte{[]byte("KEYS"), []byte("ba*")},
			},
			Expected: "0",
		},
		{
			Command: redcon.Command{
				Raw:  []byte("KEYS *oo"),
				Args: [][]byte{[]byte("KEYS"), []byte("*oo")},
			},
			Expected: "0",
		},
	}
	for _, testCase := range testCases {
		conn := DummyConn{}
		s.handleKeys(testCase.Command, &conn)
		if testCase.Expected != conn.Result {
			t.Fatalf("s.handleKeys failed: expected '%s', got '%s'", testCase.Expected, conn.Result)
		}
	}
}

type TestCase struct {
	Command  redcon.Command
	Expected string
}

type DummyConn struct {
	Result string
}

func (dc *DummyConn) RemoteAddr() string {
	return ""
}
func (dc *DummyConn) Close() error {
	return nil
}
func (dc *DummyConn) WriteError(msg string)  {}
func (dc *DummyConn) WriteString(str string) {}
func (dc *DummyConn) WriteBulk(bulk []byte) {
	dc.Result += "," + string(bulk)
}
func (dc *DummyConn) WriteBulkString(bulk string) {}
func (dc *DummyConn) WriteInt(num int)            {}
func (dc *DummyConn) WriteInt64(num int64)        {}
func (dc *DummyConn) WriteUint64(num uint64)      {}
func (dc *DummyConn) WriteArray(count int) {
	dc.Result = strconv.Itoa(count)
}
func (dc *DummyConn) WriteNull()               {}
func (dc *DummyConn) WriteRaw(data []byte)     {}
func (dc *DummyConn) WriteAny(any interface{}) {}
func (dc *DummyConn) Context() interface{} {
	return nil
}
func (dc *DummyConn) SetContext(v interface{}) {}
func (dc *DummyConn) SetReadBuffer(bytes int)  {}
func (dc *DummyConn) Detach() redcon.DetachedConn {
	return nil
}
func (dc *DummyConn) ReadPipeline() []redcon.Command {
	return nil
}
func (dc *DummyConn) PeekPipeline() []redcon.Command {
	return nil
}
func (dc *DummyConn) NetConn() net.Conn {
	return nil
}
