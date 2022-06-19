package repli

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/codec"
	"github.com/octu0/bitcaskdb/datafile"
	"github.com/octu0/bitcaskdb/indexer"
	"github.com/octu0/bitcaskdb/runtime"
)

type testNoDataSource struct{}

func (t *testNoDataSource) FileIds() []int32 {
	return nil
}

func (t *testNoDataSource) LastIndex(int32) int64 {
	return 0
}

func (t *testNoDataSource) Header(int32, int64) (*datafile.Header, bool, error) {
	return nil, true, errors.Errorf("no header")
}

func (t *testNoDataSource) Read(int32, int64, int64) (*datafile.Entry, error) {
	return nil, errors.Errorf("no entry")
}

func TestRapliStreamEmitterStartStop(t *testing.T) {
	t.Run("start/stop", func(tt *testing.T) {
		e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
		if err := e.Start(new(testNoDataSource), "127.0.0.1", -1); err != nil {
			tt.Errorf("no error ephemeral port %+v", err)
		}
		if err := e.Stop(); err != nil {
			tt.Errorf("no error %+v", err)
		}
	})
	t.Run("start/port_already_in_use", func(tt *testing.T) {
		e1 := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
		if err := e1.Start(new(testNoDataSource), "0.0.0.0", 12345); err != nil {
			tt.Errorf("no error ephemeral port %+v", err)
		}
		e2 := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
		err := e2.Start(new(testNoDataSource), "0.0.0.0", 12345)
		if err == nil {
			tt.Errorf("already port in use!")
		}
		tt.Logf("OK error=%s", err)

		if err := e1.Stop(); err != nil {
			tt.Errorf("no error %+v", err)
		}
		if err := e2.Stop(); err != nil {
			tt.Errorf("no error %+v", err)
		}
	})
}

type testFileIdsSource struct {
	ids []int32
}

func (t *testFileIdsSource) FileIds() []int32 {
	return t.ids
}

func (t *testFileIdsSource) LastIndex(int32) int64 {
	return 0
}

func (t *testFileIdsSource) Header(int32, int64) (*datafile.Header, bool, error) {
	return nil, true, errors.Errorf("no header")
}

func (t *testFileIdsSource) Read(int32, int64, int64) (*datafile.Entry, error) {
	return nil, errors.Errorf("no entry")
}

func testRepliStreamEmitterRequestCurrentFileIds(t *testing.T) {
	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	defer e.Stop()

	s := &testFileIdsSource{
		ids: []int32{0, 1, 2, 3, 4, 5},
	}
	if err := e.Start(s, "127.0.0.1", -1); err != nil {
		t.Errorf("no error ephemeral port %+v", err)
	}

	natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
	nc, err := conn(natsUrl, t.Name())
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer nc.Close()

	t.Run("empty_request_data", func(tt *testing.T) {
		msg, err := nc.Request(SubjectCurrentFileIds, []byte{}, 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseCurrentFileIds{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder empty byte read: %s", res.Err)
	})
	t.Run("mismatch_request_data", func(tt *testing.T) {
		out := bytes.NewBuffer(nil)
		gob.NewEncoder(out).Encode(time.Time{})

		msg, err := nc.Request(SubjectCurrentFileIds, out.Bytes(), 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseCurrentFileIds{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder mismatch type read: %s", res.Err)
	})
	t.Run("data_read", func(tt *testing.T) {
		out := bytes.NewBuffer(nil)
		if err := gob.NewEncoder(out).Encode(RequestCurrentFileIds{}); err != nil {
			tt.Fatalf("no error %+v", err)
		}

		msg, err := nc.Request(SubjectCurrentFileIds, out.Bytes(), 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseCurrentFileIds{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err != "" {
			tt.Errorf("no response err: actual'%s'", res.Err)
		}
		if reflect.DeepEqual(res.FileIds, []int32{0, 1, 2, 3, 4, 5}) != true {
			tt.Errorf("mismatch fileIds actual:%v", res.FileIds)
		}
	})
}

type testLastIndexSource struct {
	index map[int32]int64
}

func (t *testLastIndexSource) FileIds() []int32 {
	return nil
}

func (t *testLastIndexSource) LastIndex(id int32) int64 {
	return t.index[id]
}

func (t *testLastIndexSource) Header(int32, int64) (*datafile.Header, bool, error) {
	return nil, true, errors.Errorf("no header")
}

func (t *testLastIndexSource) Read(int32, int64, int64) (*datafile.Entry, error) {
	return nil, errors.Errorf("no entry")
}

func testRepliStreamEmitterRequestCurrentIndex(t *testing.T) {
	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	defer e.Stop()

	s := &testLastIndexSource{
		index: map[int32]int64{
			0: 123,
			1: 50,
			2: 1,
		},
	}
	if err := e.Start(s, "127.0.0.1", -1); err != nil {
		t.Errorf("no error ephemeral port %+v", err)
	}

	natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
	nc, err := conn(natsUrl, t.Name())
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer nc.Close()

	t.Run("empty_request_data", func(tt *testing.T) {
		msg, err := nc.Request(SubjectCurrentIndex, []byte{}, 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseCurrentIndex{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder empty byte read: %s", res.Err)
	})
	t.Run("mismatch_request_data", func(tt *testing.T) {
		out := bytes.NewBuffer(nil)
		gob.NewEncoder(out).Encode(time.Time{})

		msg, err := nc.Request(SubjectCurrentIndex, out.Bytes(), 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseCurrentIndex{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder mismatch type read: %s", res.Err)
	})
	t.Run("data_read", func(tt *testing.T) {
		testcase := []struct {
			fileID      int32
			expectIndex int64
			expectErr   string
		}{
			{0, 123, ""},
			{1, 50, ""},
			{2, 1, ""},
			{999, 0, ""}, // not found is 'index = 0'
		}
		for _, tc := range testcase {
			out := bytes.NewBuffer(nil)
			if err := gob.NewEncoder(out).Encode(RequestCurrentIndex{
				FileID: tc.fileID,
			}); err != nil {
				tt.Fatalf("no error %+v", err)
			}

			msg, err := nc.Request(SubjectCurrentIndex, out.Bytes(), 1*time.Second)
			if err != nil {
				tt.Fatalf("no error %+v", err)
			}
			res := ResponseCurrentIndex{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
				tt.Fatalf("no error %+v", err)
			}
			if res.Err != tc.expectErr {
				tt.Errorf("Err expect:'%s' actual:'%s'", tc.expectErr, res.Err)
			}
			if res.Index != tc.expectIndex {
				tt.Errorf("Index expect:%d actual:%d", tc.expectIndex, res.Index)
			}
		}
	})
}

type testHeaderSource struct {
	header map[testHeaderSourceKey]testHeaderSourceValue
}

type testHeaderSourceKey struct {
	fileID int32
	index  int64
}

type testHeaderSourceValue struct {
	dh    *datafile.Header
	isEOF bool
	err   error
}

func (t *testHeaderSource) FileIds() []int32 {
	return nil
}

func (t *testHeaderSource) LastIndex(int32) int64 {
	return 0
}

func (t *testHeaderSource) Header(fileID int32, index int64) (*datafile.Header, bool, error) {
	v, ok := t.header[testHeaderSourceKey{fileID, index}]
	if ok != true {
		return nil, true, errors.Errorf("testNOTFOUND")
	}
	return v.dh, v.isEOF, v.err
}

func (t *testHeaderSource) Read(int32, int64, int64) (*datafile.Entry, error) {
	return nil, errors.Errorf("no entry")
}

func testRepliStreamEmitterRequestFetchSize(t *testing.T) {
	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	defer e.Stop()

	h := make(map[testHeaderSourceKey]testHeaderSourceValue)
	h[testHeaderSourceKey{0, 38}] = testHeaderSourceValue{
		dh:    &datafile.Header{10, 10, 10, time.Unix(10, 0), 38},
		isEOF: false,
		err:   nil,
	}
	h[testHeaderSourceKey{0, 76}] = testHeaderSourceValue{
		dh:    &datafile.Header{20, 20, 20, time.Unix(20, 0), 38},
		isEOF: true,
		err:   nil,
	}
	h[testHeaderSourceKey{100, 100}] = testHeaderSourceValue{
		dh:    nil,
		isEOF: true,
		err:   errors.Errorf("testErr"),
	}

	s := &testHeaderSource{h}
	if err := e.Start(s, "127.0.0.1", -1); err != nil {
		t.Errorf("no error ephemeral port %+v", err)
	}

	natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
	nc, err := conn(natsUrl, t.Name())
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer nc.Close()

	t.Run("empty_request_data", func(tt *testing.T) {
		msg, err := nc.Request(SubjectFetchSize, []byte{}, 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseFetchSize{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder empty byte read: %s", res.Err)
	})
	t.Run("mismatch_request_data", func(tt *testing.T) {
		out := bytes.NewBuffer(nil)
		gob.NewEncoder(out).Encode(time.Time{})

		msg, err := nc.Request(SubjectFetchSize, out.Bytes(), 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseFetchSize{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder mismatch type read: %s", res.Err)
	})
	t.Run("data_read", func(tt *testing.T) {
		testcase := []struct {
			key         testHeaderSourceKey
			expectSize  int64
			expectIsEOF bool
			expectErr   string
		}{
			{
				key:         testHeaderSourceKey{0, 38},
				expectSize:  38,
				expectIsEOF: false,
				expectErr:   "",
			},
			{
				key:         testHeaderSourceKey{0, 76},
				expectSize:  38,
				expectIsEOF: true,
				expectErr:   "",
			},
			{
				key:         testHeaderSourceKey{100, 100},
				expectSize:  -1,
				expectIsEOF: true,
				expectErr:   "testErr",
			},
			{
				key:         testHeaderSourceKey{999, 9999}, // not found
				expectSize:  -1,
				expectIsEOF: true,
				expectErr:   "testNOTFOUND",
			},
		}
		for _, tc := range testcase {
			out := bytes.NewBuffer(nil)
			if err := gob.NewEncoder(out).Encode(RequestFetchSize{
				FileID: tc.key.fileID,
				Index:  tc.key.index,
			}); err != nil {
				tt.Fatalf("no error %+v", err)
			}
			msg, err := nc.Request(SubjectFetchSize, out.Bytes(), 1*time.Second)
			if err != nil {
				tt.Fatalf("no error %+v", err)
			}
			res := ResponseFetchSize{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
				tt.Fatalf("no error %+v", err)
			}
			if res.Err != tc.expectErr {
				tt.Errorf("Err expect:'%s' actual:'%s'", tc.expectErr, res.Err)
			}
			if res.Size != tc.expectSize {
				tt.Errorf("TotalSize expect:%d actual:%d", tc.expectSize, res.Size)
			}
			if res.EOF != tc.expectIsEOF {
				tt.Errorf("EOF expect:%v actual:%v", tc.expectIsEOF, res.EOF)
			}
		}
	})
}

type testReadSource struct {
	read map[testReadSourceKey]testReadSourceValue
}

type testReadSourceKey struct {
	fileID int32
	index  int64
	size   int64
}

type testReadSourceValue struct {
	de  *datafile.Entry
	err error
}

func (t *testReadSource) FileIds() []int32 {
	return nil
}

func (t *testReadSource) LastIndex(int32) int64 {
	return 0
}

func (t *testReadSource) Header(int32, int64) (*datafile.Header, bool, error) {
	return nil, true, errors.Errorf("testNOTFOUND")
}

func (t *testReadSource) Read(fileID int32, index int64, size int64) (*datafile.Entry, error) {
	v, ok := t.read[testReadSourceKey{fileID, index, size}]
	if ok != true {
		return nil, errors.Errorf("testNOTFOUND")
	}
	return v.de, v.err
}

func testRepliStreamEmitterRequestFetchData(t *testing.T) {
	maxSize := 8 * 1024
	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", int32(maxSize))
	defer e.Stop()

	dir, err := os.MkdirTemp("", "test*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(dir)

	df, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(dir),
		datafile.FileID(0),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df.Close()

	largeData := bytes.Repeat([]byte("largedata"), 8196)
	df.Write([]byte("test1"), bytes.NewReader([]byte("smalldata")), time.Time{})
	df.Write([]byte("test2"), bytes.NewReader([]byte("smalldata2")), time.Time{})
	df.Write([]byte("test3test3"), bytes.NewReader(largeData), time.Time{})
	df.Write([]byte("test4"), bytes.NewReader([]byte("smalldata4")), time.Time{})

	entry1, _ := df.Read()
	entry2, _ := df.Read()
	entry3, _ := df.Read()
	entry4, _ := df.Read()

	r := make(map[testReadSourceKey]testReadSourceValue)
	r[testReadSourceKey{0, 0, entry1.TotalSize}] = testReadSourceValue{
		de:  entry1,
		err: nil,
	}
	r[testReadSourceKey{0, entry1.TotalSize, entry1.TotalSize + entry2.TotalSize}] = testReadSourceValue{
		de:  entry2,
		err: nil,
	}
	r[testReadSourceKey{0, entry1.TotalSize + entry2.TotalSize, entry1.TotalSize + entry2.TotalSize + entry3.TotalSize}] = testReadSourceValue{
		de:  entry3,
		err: nil,
	}
	r[testReadSourceKey{0, entry1.TotalSize + entry2.TotalSize + entry3.TotalSize, entry1.TotalSize + entry2.TotalSize + entry3.TotalSize + entry4.TotalSize}] = testReadSourceValue{
		de:  entry4,
		err: nil,
	}
	r[testReadSourceKey{100, 0, 100}] = testReadSourceValue{
		de:  nil,
		err: errors.Errorf("testErr"),
	}

	s := &testReadSource{r}
	if err := e.Start(s, "127.0.0.1", -1); err != nil {
		t.Errorf("no error ephemeral port %+v", err)
	}

	natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
	nc, err := conn(natsUrl, t.Name())
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer nc.Close()

	t.Run("empty_request_data", func(tt *testing.T) {
		msg, err := nc.Request(SubjectFetchData, []byte{}, 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseFetchData{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder empty byte read: %s", res.Err)
	})
	t.Run("mismatch_request_data", func(tt *testing.T) {
		out := bytes.NewBuffer(nil)
		gob.NewEncoder(out).Encode(time.Time{})

		msg, err := nc.Request(SubjectFetchData, out.Bytes(), 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseFetchData{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		if res.Err == "" {
			tt.Errorf("not empty error")
		}
		tt.Logf("decoder mismatch type read: %s", res.Err)
	})
	t.Run("data_read/single", func(tt *testing.T) {
		testcase := []struct {
			key           testReadSourceKey
			expectPartKey []string
			expectData    []byte
			expectErr     string
			single        bool
		}{
			{
				key:           testReadSourceKey{0, 0, 38},
				single:        true,
				expectPartKey: nil,
				expectData:    []byte("smalldata"),
				expectErr:     "",
			},
			{
				key:           testReadSourceKey{0, 38, 77},
				single:        true,
				expectPartKey: nil,
				expectData:    []byte("smalldata2"),
				expectErr:     "",
			},
			{
				key:           testReadSourceKey{0, 77, 73875},
				single:        false,
				expectPartKey: nil, // skip: data_read/partial
				expectData:    nil, // skip: data_read/partial
				expectErr:     "",
			},
			{
				key:           testReadSourceKey{0, 73875, 73914},
				single:        true,
				expectPartKey: nil,
				expectData:    []byte("smalldata4"),
				expectErr:     "",
			},
			{
				key:           testReadSourceKey{100, 0, 100},
				single:        true,
				expectPartKey: nil,
				expectData:    nil,
				expectErr:     "testErr",
			},
		}
		for _, tc := range testcase {
			if tc.single != true {
				continue
			}

			tt.Logf("req fileID=%d index=%d size=%d", tc.key.fileID, tc.key.index, tc.key.size)
			out := bytes.NewBuffer(nil)
			if err := gob.NewEncoder(out).Encode(RequestFetchData{
				FileID: tc.key.fileID,
				Index:  tc.key.index,
				Size:   tc.key.size,
			}); err != nil {
				tt.Fatalf("no error %+v", err)
			}
			msg, err := nc.Request(SubjectFetchData, out.Bytes(), 1*time.Second)
			if err != nil {
				tt.Fatalf("no error %+v", err)
			}
			res := ResponseFetchData{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
				tt.Fatalf("no error %+v", err)
			}
			if reflect.DeepEqual(res.PartKeys, tc.expectPartKey) != true {
				tt.Errorf("expect:%v actual:%v", tc.expectPartKey, res.PartKeys)
			}
			if bytes.Equal(res.EntryValue, tc.expectData) != true {
				tt.Errorf("expect:%s actual:%s", tc.expectData, res.EntryValue)
			}
			if res.Err != tc.expectErr {
				tt.Errorf("expect:%s actual:%s", tc.expectErr, res.Err)
			}
		}
	})
	t.Run("data_read/partial", func(tt *testing.T) {
		out := bytes.NewBuffer(nil)
		if err := gob.NewEncoder(out).Encode(RequestFetchData{
			FileID: 0,
			Index:  77,
			Size:   73875,
		}); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		msg, err := nc.Request(SubjectFetchData, out.Bytes(), 1*time.Second)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		res := ResponseFetchData{}
		if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
			tt.Fatalf("no error %+v", err)
		}
		expectKeys := []string{
			"part.test3test3.p.000000000-000004096",
			"part.test3test3.p.000004096-000008192",
			"part.test3test3.p.000008192-000012288",
			"part.test3test3.p.000012288-000016384",
			"part.test3test3.p.000016384-000020480",
			"part.test3test3.p.000020480-000024576",
			"part.test3test3.p.000024576-000028672",
			"part.test3test3.p.000028672-000032768",
			"part.test3test3.p.000032768-000036864",
			"part.test3test3.p.000036864-000040960",
			"part.test3test3.p.000040960-000045056",
			"part.test3test3.p.000045056-000049152",
			"part.test3test3.p.000049152-000053248",
			"part.test3test3.p.000053248-000057344",
			"part.test3test3.p.000057344-000061440",
			"part.test3test3.p.000061440-000065536",
			"part.test3test3.p.000065536-000069632",
			"part.test3test3.p.000069632-000073728",
			"part.test3test3.p.000073728-000073764",
		}
		if reflect.DeepEqual(res.PartKeys, expectKeys) != true {
			tt.Errorf("key mismatch")
		}
		if res.EntryValue != nil {
			tt.Errorf("no data: %+v", res.EntryValue)
		}
		if res.Err != "" {
			tt.Errorf("no error: %+v", res.Err)
		}

		tmp := bytes.NewBuffer(nil)
		c := crc32.New(crc32.IEEETable)
		for i, key := range res.PartKeys {
			msg, err := nc.Request(key, []byte{}, 1*time.Second)
			if err != nil {
				tt.Fatalf("[%d]no error %+v", i, err)
			}
			re := PartialData{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&re); err != nil {
				tt.Fatalf("[%d]no error %+v", i, err)
			}
			if re.Err != "" {
				tt.Errorf("[%d]no err %s", i, re.Err)
			}
			tmp.Write(re.Data)
			c.Write(re.Data)
		}
		if c.Sum32() != res.Checksum {
			tt.Errorf("response checksum:%d written:%d", res.Checksum, c.Sum32())
		}
		if bytes.Equal(largeData, tmp.Bytes()) != true {
			tt.Errorf("broken")
		}
	})
}

func TestRepliStreamEmitterRequest(t *testing.T) {
	t.Run("current_file_ids", testRepliStreamEmitterRequestCurrentFileIds)
	t.Run("current_index", testRepliStreamEmitterRequestCurrentIndex)
	t.Run("fetch_size", testRepliStreamEmitterRequestFetchSize)
	t.Run("fetch_data", testRepliStreamEmitterRequestFetchData)
}

type testDatafileSource struct {
	df datafile.Datafile
}

func (t *testDatafileSource) FileIds() []int32 {
	return []int32{t.df.FileID()}
}

func (t *testDatafileSource) LastIndex(fileID int32) int64 {
	if fileID == t.df.FileID() {
		return t.df.Size()
	}
	return 0
}

func (t *testDatafileSource) Header(fileID int32, index int64) (*datafile.Header, bool, error) {
	if fileID == t.df.FileID() {
		return t.df.ReadAtHeader(index)
	}
	return nil, true, errors.Errorf("no header")
}

func (t *testDatafileSource) Read(fileID int32, index int64, size int64) (*datafile.Entry, error) {
	if fileID == t.df.FileID() {
		return t.df.ReadAt(index, size)
	}
	return nil, errors.Errorf("no entry")
}

func TestRepliStreamEmitterRepli(t *testing.T) {
	t.Run("delete", func(tt *testing.T) {
		e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
		defer e.Stop()

		if err := e.Start(new(testNoDataSource), "127.0.0.1", -1); err != nil {
			t.Errorf("no error ephemeral port %+v", err)
		}

		natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
		nc, err := conn(natsUrl, tt.Name())
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer nc.Close()

		data := make([]RepliData, 0)
		m := new(sync.Mutex)
		nc.Subscribe(SubjectRepli, func(msg *nats.Msg) {
			res := RepliData{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
				tt.Fatalf("no error %+v", err)
				return
			}

			m.Lock()
			defer m.Unlock()
			data = append(data, res)
		})

		e.EmitDelete([]byte("test01"))
		time.Sleep(100 * time.Millisecond)

		if len(data) != 1 {
			tt.Errorf("emit test01 actual=%d", len(data))
		}
		if data[0].IsDelete != true {
			tt.Errorf("emitDelete test01")
		}
		if bytes.Equal(data[0].EntryKey, []byte("test01")) != true {
			tt.Errorf("emitDelete test01")
		}
		if (data[0].EntryValue == nil && data[0].PartKeys == nil) != true {
			tt.Errorf("delete payload key only")
		}

		e.EmitDelete([]byte("test02"))
		e.EmitDelete([]byte("test03"))

		time.Sleep(100 * time.Millisecond)

		if len(data) != 3 {
			tt.Errorf("emit test01 actual=%d", len(data))
		}
		if data[1].IsDelete != true {
			tt.Errorf("emitDelete test02")
		}
		if bytes.Equal(data[1].EntryKey, []byte("test02")) != true {
			tt.Errorf("emitDelete test02")
		}
		if (data[1].EntryValue == nil && data[1].PartKeys == nil) != true {
			tt.Errorf("delete payload key only")
		}
		if data[2].IsDelete != true {
			tt.Errorf("emitDelete test03")
		}
		if bytes.Equal(data[2].EntryKey, []byte("test03")) != true {
			tt.Errorf("emitDelete test03")
		}
		if (data[2].EntryValue == nil && data[2].PartKeys == nil) != true {
			tt.Errorf("delete payload key only")
		}
	})
	t.Run("insert/single", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "test*")
		if err != nil {
			tt.Fatalf("%+v", err)
		}
		defer os.RemoveAll(dir)

		df, err := datafile.Open(
			datafile.RuntimeContext(runtime.DefaultContext()),
			datafile.Path(dir),
			datafile.FileID(0),
			datafile.FileMode(os.FileMode(0600)),
			datafile.TempDir(""),
			datafile.CopyTempThreshold(0),
		)
		if err != nil {
			tt.Fatalf("%+v", err)
		}
		defer df.Close()

		e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
		if err := e.Start(&testDatafileSource{df}, "127.0.0.1", -1); err != nil {
			t.Errorf("no error ephemeral port %+v", err)
		}
		defer e.Stop()

		i1, s1, _ := df.Write([]byte("test1"), bytes.NewReader([]byte("smalldata")), time.Time{})
		i2, s2, _ := df.Write([]byte("test2"), bytes.NewReader([]byte("smalldata2")), time.Time{})

		natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
		nc, err := conn(natsUrl, tt.Name())
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer nc.Close()

		data := make([]RepliData, 0)
		m := new(sync.Mutex)
		nc.Subscribe(SubjectRepli, func(msg *nats.Msg) {
			res := RepliData{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
				tt.Fatalf("no error %+v", err)
				return
			}

			m.Lock()
			defer m.Unlock()
			data = append(data, res)
		})

		e.EmitInsert(indexer.Filer{
			FileID: 0,
			Index:  i1,
			Size:   s1,
		})
		time.Sleep(100 * time.Millisecond)

		if len(data) != 1 {
			tt.Errorf("emit test1 actual=%d", len(data))
		}
		if data[0].IsDelete {
			tt.Errorf("emitInsert test1")
		}
		if bytes.Equal(data[0].EntryKey, []byte("test1")) != true {
			tt.Errorf("emitInsert key actual: %v", data[0].EntryKey)
		}
		if bytes.Equal(data[0].EntryValue, []byte("smalldata")) != true {
			tt.Errorf("emitInsert test1 smalldata: actual=%s", data[0].EntryValue)
		}

		e.EmitInsert(indexer.Filer{
			FileID: 0,
			Index:  i2,
			Size:   s2,
		})
		time.Sleep(100 * time.Millisecond)

		if len(data) != 2 {
			tt.Errorf("emit test2 actual=%d", len(data))
		}
		if data[1].IsDelete {
			tt.Errorf("emitInsert test2")
		}
		if bytes.Equal(data[1].EntryKey, []byte("test2")) != true {
			tt.Errorf("emitInsert key is nil")
		}
		if bytes.Equal(data[1].EntryValue, []byte("smalldata2")) != true {
			tt.Errorf("emitInsert test2 smalldata2")
		}
	})
	t.Run("insert/partial", func(tt *testing.T) {
		dir, err := os.MkdirTemp("", "test*")
		if err != nil {
			tt.Fatalf("%+v", err)
		}
		defer os.RemoveAll(dir)

		df, err := datafile.Open(
			datafile.RuntimeContext(runtime.DefaultContext()),
			datafile.Path(dir),
			datafile.FileID(0),
			datafile.FileMode(os.FileMode(0600)),
			datafile.TempDir(""),
			datafile.CopyTempThreshold(0),
		)
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer df.Close()

		largeData := bytes.Repeat([]byte("largedata"), 8196)
		i1, s1, _ := df.Write([]byte("testlarge1"), bytes.NewReader(largeData), time.Time{})
		i2, s2, _ := df.Write([]byte("testsmall1"), bytes.NewReader([]byte("small")), time.Time{})

		maxSize := 8 * 1024
		e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", int32(maxSize))
		if err := e.Start(&testDatafileSource{df}, "127.0.0.1", -1); err != nil {
			t.Errorf("no error ephemeral port %+v", err)
		}
		defer e.Stop()

		natsUrl := fmt.Sprintf("nats://%s", e.server.Addr().String())
		nc, err := conn(natsUrl, tt.Name())
		if err != nil {
			tt.Fatalf("no error %+v", err)
		}
		defer nc.Close()

		data := make([]RepliData, 0)
		m := new(sync.Mutex)
		nc.Subscribe(SubjectRepli, func(msg *nats.Msg) {
			res := RepliData{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&res); err != nil {
				tt.Fatalf("no error %+v", err)
				return
			}

			m.Lock()
			defer m.Unlock()
			data = append(data, res)
		})

		e.EmitInsert(indexer.Filer{
			FileID: 0,
			Index:  i1,
			Size:   s1,
		})
		time.Sleep(100 * time.Millisecond)

		if len(data) != 1 {
			tt.Errorf("emit testlarge1 actual=%d", len(data))
		}
		if data[0].IsDelete {
			tt.Errorf("emitInsert testlarge1")
		}
		if bytes.Equal(data[0].EntryKey, []byte("testlarge1")) != true {
			tt.Errorf("emitInsert key actual:%s", data[0].EntryKey)
		}
		expectKeys := []string{
			"part.testlarge1.p.000000000-000004096",
			"part.testlarge1.p.000004096-000008192",
			"part.testlarge1.p.000008192-000012288",
			"part.testlarge1.p.000012288-000016384",
			"part.testlarge1.p.000016384-000020480",
			"part.testlarge1.p.000020480-000024576",
			"part.testlarge1.p.000024576-000028672",
			"part.testlarge1.p.000028672-000032768",
			"part.testlarge1.p.000032768-000036864",
			"part.testlarge1.p.000036864-000040960",
			"part.testlarge1.p.000040960-000045056",
			"part.testlarge1.p.000045056-000049152",
			"part.testlarge1.p.000049152-000053248",
			"part.testlarge1.p.000053248-000057344",
			"part.testlarge1.p.000057344-000061440",
			"part.testlarge1.p.000061440-000065536",
			"part.testlarge1.p.000065536-000069632",
			"part.testlarge1.p.000069632-000073728",
			"part.testlarge1.p.000073728-000073764",
		}
		if reflect.DeepEqual(data[0].PartKeys, expectKeys) != true {
			tt.Errorf("key mismatch")
		}
		if data[0].EntryValue != nil {
			tt.Errorf("largedata to partial")
		}
		tmp := bytes.NewBuffer(nil)
		c := crc32.New(crc32.IEEETable)
		for i, key := range data[0].PartKeys {
			msg, err := nc.Request(key, []byte{}, 1*time.Second)
			if err != nil {
				tt.Fatalf("[%d]no error %+v", i, err)
			}
			re := PartialData{}
			if err := gob.NewDecoder(bytes.NewReader(msg.Data)).Decode(&re); err != nil {
				tt.Fatalf("[%d]no error %+v", i, err)
			}
			if re.Err != "" {
				tt.Errorf("[%d]no err %s", i, re.Err)
			}
			tmp.Write(re.Data)
			c.Write(re.Data)
		}
		if c.Sum32() != data[0].Checksum {
			tt.Errorf("response checksum:%d written:%d", data[0].Checksum, c.Sum32())
		}
		if bytes.Equal(largeData, tmp.Bytes()) != true {
			tt.Errorf("broken")
		}

		e.EmitInsert(indexer.Filer{
			FileID: 0,
			Index:  i2,
			Size:   s2,
		})
		time.Sleep(100 * time.Millisecond)

		if len(data) != 2 {
			tt.Errorf("emit testsmall actual=%d", len(data))
		}
		if data[1].IsDelete {
			tt.Errorf("emitInsert testsmall1")
		}
		if bytes.Equal(data[1].EntryKey, []byte("testsmall1")) != true {
			tt.Errorf("emitInsert key actual:%s", data[1].EntryKey)
		}
		if bytes.Equal(data[1].EntryValue, []byte("small")) != true {
			tt.Errorf("emitInsert testsmall1 small")
		}
	})
}

func TestRepliStreamEmitterReleaseLoop(t *testing.T) {
	releaseCount := int32(0)

	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	defer e.Stop()

	e.releaseTTL = 1 * time.Second
	if err := e.Start(new(testNoDataSource), "127.0.0.1", -1); err != nil {
		t.Errorf("no error ephemeral port %+v", err)
	}
	e.addRelease(func() {
		atomic.AddInt32(&releaseCount, 1)
	})
	e.addRelease(func() {
		atomic.AddInt32(&releaseCount, 1)
	})

	time.Sleep(e.releaseTTL)

	if c := atomic.LoadInt32(&releaseCount); c != 0 {
		t.Errorf("not yet runRelease: actual=%d", c)
	}

	time.Sleep(e.releaseTTL)

	if c := atomic.LoadInt32(&releaseCount); c != 2 {
		t.Errorf("2 release run: acutal=%d", c)
	}

	e.addRelease(func() {
		atomic.AddInt32(&releaseCount, 1)
	})

	time.Sleep(e.releaseTTL)

	if c := atomic.LoadInt32(&releaseCount); c != 2 {
		t.Errorf("not yet runRelease: actual=%d", c)
	}

	time.Sleep(e.releaseTTL)

	if c := atomic.LoadInt32(&releaseCount); c != 3 {
		t.Errorf("release run + 1: actual=%d", c)
	}
}

type testNoDataDestination struct {
}

func (t *testNoDataDestination) LastFiles() []FileIDAndIndex {
	return nil
}

func (t *testNoDataDestination) Insert(int32, int64, uint32, []byte, io.Reader, time.Time) error {
	return nil
}

func (t *testNoDataDestination) Delete([]byte) error {
	return nil
}

func TestRepliStreamReciverStartStop(t *testing.T) {
	t.Run("start/stop", func(tt *testing.T) {
		e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
		defer e.Stop()
		if err := e.Start(new(testNoDataSource), "127.0.0.1", 4220); err != nil {
			tt.Errorf("no error %+v", err)
		}

		r := NewStreamReciver(runtime.DefaultContext(), log.Default(), "", 0)
		if err := r.Start(new(testNoDataDestination), "127.0.0.1", 4220); err != nil {
			tt.Errorf("no error %+v", err)
		}
		if err := r.Stop(); err != nil {
			tt.Errorf("no error %+v", err)
		}
	})
	t.Run("start/server_not_found", func(tt *testing.T) {
		r := NewStreamReciver(runtime.DefaultContext(), log.Default(), "", 0)
		err := r.Start(new(testNoDataDestination), "127.0.0.1", 12345)
		if err == nil {
			tt.Errorf("must error")
		}
		tt.Logf("OK error=%s", err)

		if err := r.Stop(); err != nil {
			tt.Errorf("no error %+v", err)
		}
	})
}

type testRepliStreamReciverSrcCounter struct {
	countFileIds   int
	countLastIndex int
	countHeader    int
	countRead      int
}

func (t *testRepliStreamReciverSrcCounter) FileIds() []int32 {
	t.countFileIds += 1
	return nil
}

func (t *testRepliStreamReciverSrcCounter) LastIndex(int32) int64 {
	t.countLastIndex += 1
	return 0
}

func (t *testRepliStreamReciverSrcCounter) Header(int32, int64) (*datafile.Header, bool, error) {
	t.countHeader += 1
	return nil, true, errors.Errorf("no header")
}

func (t *testRepliStreamReciverSrcCounter) Read(int32, int64, int64) (*datafile.Entry, error) {
	t.countRead += 1
	return nil, errors.Errorf("no entry")
}

type testRepliStreamReciverDestCounter struct {
	countLastFiles int
	countInsert    int
	countDelete    int
}

func (t *testRepliStreamReciverDestCounter) LastFiles() []FileIDAndIndex {
	t.countLastFiles += 1
	return nil
}

func (t *testRepliStreamReciverDestCounter) Insert(int32, int64, uint32, []byte, io.Reader, time.Time) error {
	t.countInsert += 1
	return nil
}

func (t *testRepliStreamReciverDestCounter) Delete([]byte) error {
	t.countDelete += 1
	return nil
}

func testRepliStreamReciverRepliEmptySourceEmptyDestination(t *testing.T) {
	src := new(testRepliStreamReciverSrcCounter)
	dst := new(testRepliStreamReciverDestCounter)

	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	if err := e.Start(src, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}

	r := NewStreamReciver(runtime.DefaultContext(), log.Default(), "", 0)
	if err := r.Start(dst, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := r.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := e.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}

	if src.countFileIds != 1 {
		t.Errorf("check src file ids")
	}
	if src.countLastIndex != 0 {
		t.Errorf("no request cause dest files is zero")
	}
	if src.countHeader != 0 {
		t.Errorf("no request")
	}
	if src.countRead != 0 {
		t.Errorf("no request")
	}
	if dst.countLastFiles != 1 {
		t.Errorf("check dst file ids")
	}
	if dst.countInsert != 0 {
		t.Errorf("no request")
	}
	if dst.countDelete != 0 {
		t.Errorf("no request")
	}
}

type testRepliStreamReciverDatafileSource struct {
	dfs            []datafile.Datafile
	countFileIds   int
	countLastIndex int
	countHeader    int
	countRead      int
}

func (t *testRepliStreamReciverDatafileSource) FileIds() []int32 {
	t.countFileIds += 1

	ids := make([]int32, len(t.dfs))
	for i, df := range t.dfs {
		ids[i] = df.FileID()
	}
	return ids
}

func (t *testRepliStreamReciverDatafileSource) LastIndex(fileID int32) int64 {
	t.countLastIndex += 1
	for _, df := range t.dfs {
		if df.FileID() == fileID {
			return df.Size()
		}
	}
	return 0
}

func (t *testRepliStreamReciverDatafileSource) Header(fileID int32, index int64) (*datafile.Header, bool, error) {
	t.countHeader += 1
	for _, df := range t.dfs {
		if df.FileID() == fileID {
			return df.ReadAtHeader(index)
		}
	}
	return nil, true, errors.Errorf("no header")
}

func (t *testRepliStreamReciverDatafileSource) Read(fileID int32, index int64, size int64) (*datafile.Entry, error) {
	t.countRead += 1
	for _, df := range t.dfs {
		if df.FileID() == fileID {
			return df.ReadAt(index, size)
		}
	}
	return nil, errors.Errorf("no entry")
}

type testRepliStreamReciverDestCounterAndEntries struct {
	countLastFiles int
	countInsert    int
	countDelete    int
	seq            []*testRepliStreamReciverDestCounterAndEntriesSeq
	m              sync.Mutex
}

type testRepliStreamReciverDestCounterAndEntriesSeq struct {
	isDelete bool
	key      []byte
	fileID   int32
	payload  *codec.Payload
}

func (t *testRepliStreamReciverDestCounterAndEntries) LastFiles() []FileIDAndIndex {
	t.m.Lock()
	defer t.m.Unlock()

	t.countLastFiles += 1
	return nil
}

func (t *testRepliStreamReciverDestCounterAndEntries) Insert(fileID int32, index int64, checksum uint32, key []byte, value io.Reader, expiry time.Time) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.countInsert += 1

	buf := bytes.NewBuffer(nil)
	e := codec.NewEncoder(runtime.DefaultContext(), buf, "", 0)
	defer e.Close()

	if _, err := e.Encode(key, value, expiry); err != nil {
		return err
	}

	d := codec.NewDecoder(runtime.DefaultContext(), bytes.NewReader(buf.Bytes()), 0)
	defer d.Close()

	p, err := d.Decode()
	if err != nil {
		return err
	}
	t.seq = append(t.seq, &testRepliStreamReciverDestCounterAndEntriesSeq{
		isDelete: false,
		fileID:   fileID,
		payload:  p,
	})
	return nil
}

func (t *testRepliStreamReciverDestCounterAndEntries) Delete(k []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.countDelete += 1
	t.seq = append(t.seq, &testRepliStreamReciverDestCounterAndEntriesSeq{
		isDelete: true,
		key:      k,
	})
	return nil
}

func testRepliStreamReciverRepliDestinationBehindSourceSmallData(t *testing.T) {
	df1Dir, err := os.MkdirTemp("", "testdf1*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df1Dir)

	df2Dir, err := os.MkdirTemp("", "testdf2*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df2Dir)

	df3Dir, err := os.MkdirTemp("", "testdf3*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df3Dir)

	df1, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df1Dir),
		datafile.FileID(0),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df1.Close()

	df2, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df2Dir),
		datafile.FileID(1),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df2.Close()

	df3, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df3Dir),
		datafile.FileID(2),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df3.Close()

	df1.Write([]byte("test1"), bytes.NewReader([]byte("smalldata1")), time.Unix(1, 0))
	df1.Write([]byte("test2"), bytes.NewReader([]byte("smalldata12")), time.Unix(2, 0))
	df1.Write([]byte("test3"), bytes.NewReader([]byte("smalldata123")), time.Unix(3, 0))

	df2.Write([]byte("test4test4"), bytes.NewReader([]byte("t4")), time.Unix(4, 0))

	for i := 0; i < 10; i += 1 {
		k := []byte(fmt.Sprintf("k%02d", i))
		v := []byte(fmt.Sprintf("v%02d", i))
		df3.Write(k, bytes.NewReader(v), time.Unix(5+int64(i), 0))
	}

	src := &testRepliStreamReciverDatafileSource{
		dfs: []datafile.Datafile{df1, df2, df3},
	}
	dst := new(testRepliStreamReciverDestCounterAndEntries)

	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	if err := e.Start(src, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}

	r := NewStreamReciver(runtime.DefaultContext(), log.Default(), "", 0)
	if err := r.Start(dst, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := r.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := e.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}

	if src.countFileIds != 1 {
		t.Errorf("fist access")
	}
	if src.countLastIndex != 0 {
		t.Errorf("no same fileID")
	}
	if src.countHeader != 14 {
		t.Errorf("read all header")
	}
	if src.countRead != 14 {
		t.Errorf("read all entry")
	}
	if dst.countLastFiles != 1 {
		t.Errorf("first access")
	}
	if dst.countInsert != 14 {
		t.Errorf("insert all entry")
	}
	if dst.countDelete != 0 {
		t.Errorf("no delete")
	}
	expectKeys := [][]byte{
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
		[]byte("test4test4"),
		[]byte("k00"),
		[]byte("k01"),
		[]byte("k02"),
		[]byte("k03"),
		[]byte("k04"),
		[]byte("k05"),
		[]byte("k06"),
		[]byte("k07"),
		[]byte("k08"),
		[]byte("k09"),
	}
	expectValues := [][]byte{
		[]byte("smalldata1"),
		[]byte("smalldata12"),
		[]byte("smalldata123"),
		[]byte("t4"),
		[]byte("v00"),
		[]byte("v01"),
		[]byte("v02"),
		[]byte("v03"),
		[]byte("v04"),
		[]byte("v05"),
		[]byte("v06"),
		[]byte("v07"),
		[]byte("v08"),
		[]byte("v09"),
	}
	expectFileIds := []int32{
		df1.FileID(),
		df1.FileID(),
		df1.FileID(),
		df2.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
		df3.FileID(),
	}
	if len(expectKeys) != len(dst.seq) {
		t.Errorf("expect insert:%d, actual insert:%d", len(expectKeys), len(dst.seq))
	}
	for i, s := range dst.seq {
		if s.fileID != expectFileIds[i] {
			t.Errorf("[%d] fileID expect:%d actual:%d", i, expectFileIds[i], s.fileID)
		}
		if bytes.Equal(expectKeys[i], s.payload.Key) != true {
			t.Errorf("[%d] key expect:%s actual:%s", i, expectKeys[i], s.payload.Key)
		}
		buf := bytes.NewBuffer(nil)
		buf.ReadFrom(s.payload.Value)
		if bytes.Equal(expectValues[i], buf.Bytes()) != true {
			t.Errorf("[%d] value expect:%s actual:%s", i, expectValues[i], buf.Bytes())
		}
		if time.Unix(int64(i)+1, 0).UTC() != s.payload.Expiry {
			t.Errorf("[%d] expiry expect:%s actual:%s", i, time.Unix(int64(i)+1, 0).UTC(), s.payload.Expiry)
		}
	}
	for _, s := range dst.seq {
		s.payload.Close()
	}
}

func testRepliStreamReciverRepliDestinationBehindSourceLargeData(t *testing.T) {
	df1Dir, err := os.MkdirTemp("", "testdf1*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df1Dir)

	df2Dir, err := os.MkdirTemp("", "testdf2*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df2Dir)

	df3Dir, err := os.MkdirTemp("", "testdf3*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df3Dir)

	df1, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df1Dir),
		datafile.FileID(0),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df1.Close()

	df2, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df2Dir),
		datafile.FileID(1),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df2.Close()

	df3, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df3Dir),
		datafile.FileID(2),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df3.Close()

	largeData1 := bytes.Repeat([]byte("t"), 8192)
	largeData2 := bytes.Repeat([]byte("t"), 10240)
	largeData3 := bytes.Repeat([]byte("t"), 5000)
	largeData4 := bytes.Repeat([]byte("t"), 10000)
	largeData5 := bytes.Repeat([]byte("t"), 20480)
	largeData6 := bytes.Repeat([]byte("t"), 8193)

	df1.Write([]byte("test1"), bytes.NewReader(largeData1), time.Unix(1, 0))
	df1.Write([]byte("test2"), bytes.NewReader(largeData2), time.Unix(2, 0))
	df1.Write([]byte("test3"), bytes.NewReader(largeData3), time.Unix(3, 0))

	df2.Write([]byte("test4test4"), bytes.NewReader(largeData4), time.Unix(4, 0))

	df3.Write([]byte("t5"), bytes.NewReader(largeData5), time.Unix(5, 0))
	df3.Write([]byte("te6"), bytes.NewReader(largeData6), time.Unix(6, 0))

	src := &testRepliStreamReciverDatafileSource{
		dfs: []datafile.Datafile{df1, df2, df3},
	}
	dst := new(testRepliStreamReciverDestCounterAndEntries)

	maxSize := 8 * 1024
	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", int32(maxSize))
	if err := e.Start(src, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}

	r := NewStreamReciver(runtime.DefaultContext(), log.Default(), "", 0)
	if err := r.Start(dst, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := r.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := e.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}

	if src.countFileIds != 1 {
		t.Errorf("fist access")
	}
	if src.countLastIndex != 0 {
		t.Errorf("no same fileID")
	}
	if src.countHeader != 6 {
		t.Errorf("read all header")
	}
	if src.countRead != 6 {
		t.Errorf("read all entry")
	}
	if dst.countLastFiles != 1 {
		t.Errorf("first access")
	}
	if dst.countInsert != 6 {
		t.Errorf("insert all entry")
	}
	if dst.countDelete != 0 {
		t.Errorf("no delete")
	}
	expectKeys := [][]byte{
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
		[]byte("test4test4"),
		[]byte("t5"),
		[]byte("te6"),
	}
	expectValues := [][]byte{
		largeData1,
		largeData2,
		largeData3,
		largeData4,
		largeData5,
		largeData6,
	}
	expectFileIds := []int32{
		df1.FileID(),
		df1.FileID(),
		df1.FileID(),
		df2.FileID(),
		df3.FileID(),
		df3.FileID(),
	}
	if len(expectKeys) != len(dst.seq) {
		t.Errorf("expect insert:%d, actual insert:%d", len(expectKeys), len(dst.seq))
	}
	for i, s := range dst.seq {
		if expectFileIds[i] != s.fileID {
			t.Errorf("[%d] fileID expect:%d actual:%d", i, expectFileIds[i], s.fileID)
		}
		if bytes.Equal(expectKeys[i], s.payload.Key) != true {
			t.Errorf("[%d] key expect:%s actual:%s", i, expectKeys[i], s.payload.Key)
		}
		buf := bytes.NewBuffer(nil)
		buf.ReadFrom(s.payload.Value)
		if bytes.Equal(expectValues[i], buf.Bytes()) != true {
			t.Errorf("[%d] value expect size:%d actual size:%d", i, len(expectValues[i]), buf.Len())
		}
		if time.Unix(int64(i+1), 0).UTC() != s.payload.Expiry {
			t.Errorf("[%d] expiry expect:%s actual:%s", i, time.Unix(int64(i+1), 0).UTC(), s.payload.Expiry)
		}
	}
	for _, s := range dst.seq {
		s.payload.Close()
	}
}

func testRepliStreamReciverRepliDestinationBehindSourceProcessing(t *testing.T) {
	df1Dir, err := os.MkdirTemp("", "testdf1*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df1Dir)

	df2Dir, err := os.MkdirTemp("", "testdf2*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df2Dir)

	df3Dir, err := os.MkdirTemp("", "testdf3*")
	if err != nil {
		t.Fatalf("no error %+v", err)
	}
	defer os.RemoveAll(df3Dir)

	df1, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df1Dir),
		datafile.FileID(0),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df1.Close()

	df2, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df2Dir),
		datafile.FileID(1),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df2.Close()

	df3, err := datafile.Open(
		datafile.RuntimeContext(runtime.DefaultContext()),
		datafile.Path(df3Dir),
		datafile.FileID(2),
		datafile.FileMode(os.FileMode(0600)),
		datafile.TempDir(""),
		datafile.CopyTempThreshold(0),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer df3.Close()

	df1.Write([]byte("test1"), bytes.NewReader([]byte("smalldata1")), time.Unix(1, 0))
	df1.Write([]byte("test2"), bytes.NewReader([]byte("smalldata12")), time.Unix(2, 0))
	df1.Write([]byte("test3"), bytes.NewReader([]byte("smalldata123")), time.Unix(3, 0))

	df2.Write([]byte("test4test4"), bytes.NewReader([]byte("t4")), time.Unix(4, 0))

	for i := 0; i < 5; i += 1 {
		k := []byte(fmt.Sprintf("k%02d", i))
		v := []byte(fmt.Sprintf("v%02d", i))
		df3.Write(k, bytes.NewReader(v), time.Unix(5+int64(i), 0))
	}

	src := &testRepliStreamReciverDatafileSource{
		dfs: []datafile.Datafile{df1, df2, df3},
	}
	dst := new(testRepliStreamReciverDestCounterAndEntries)

	e := NewStreamEmitter(runtime.DefaultContext(), log.Default(), "", 0)
	if err := e.Start(src, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}

	r := NewStreamReciver(runtime.DefaultContext(), log.Default(), "", 0)
	if err := r.Start(dst, "127.0.0.1", 4220); err != nil {
		t.Errorf("no error %+v", err)
	}

	i1, s1, _ := df1.Write([]byte("test123"), bytes.NewReader([]byte("value123")), time.Unix(123, 0))
	e.EmitInsert(indexer.Filer{
		FileID: df1.FileID(),
		Index:  i1,
		Size:   s1,
	})
	e.EmitDelete([]byte("test1"))
	i2, s2, _ := df1.Write([]byte("test1234"), bytes.NewReader([]byte("value1234")), time.Unix(1234, 0))
	e.EmitInsert(indexer.Filer{
		FileID: df1.FileID(),
		Index:  i2,
		Size:   s2,
	})
	e.EmitDelete([]byte("test2"))
	i3, s3, _ := df1.Write([]byte("test12345"), bytes.NewReader([]byte("value12345")), time.Unix(12345, 0))
	e.EmitInsert(indexer.Filer{
		FileID: df1.FileID(),
		Index:  i3,
		Size:   s3,
	})
	i4, s4, _ := df2.Write([]byte("test123456"), bytes.NewReader([]byte("value123456")), time.Unix(123456, 0))
	e.EmitInsert(indexer.Filer{
		FileID: df2.FileID(),
		Index:  i4,
		Size:   s4,
	})

	if err := r.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}
	if err := e.Stop(); err != nil {
		t.Errorf("no error %+v", err)
	}

	if src.countFileIds != 1 {
		t.Errorf("fist access")
	}
	if src.countLastIndex != 0 {
		t.Errorf("no same fileID")
	}
	if src.countHeader != 9 {
		t.Errorf("read all header")
	}
	if src.countRead != 13 {
		t.Errorf("read all entry")
	}
	if dst.countLastFiles != 1 {
		t.Errorf("first access")
	}
	if dst.countInsert != 13 {
		t.Errorf("insert all entry %v", dst.countInsert)
	}
	if dst.countDelete != 2 {
		t.Errorf("no delete")
	}
	expectSeq := []struct {
		isDelete     bool
		deleteKey    []byte
		insertKey    []byte
		insertValue  []byte
		insertExpiry time.Time
		insertFileID int32
	}{
		{
			isDelete:     false,
			insertKey:    []byte("test1"),
			insertValue:  []byte("smalldata1"),
			insertExpiry: time.Unix(1, 0).UTC(),
			insertFileID: df1.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test2"),
			insertValue:  []byte("smalldata12"),
			insertExpiry: time.Unix(2, 0).UTC(),
			insertFileID: df1.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test3"),
			insertValue:  []byte("smalldata123"),
			insertExpiry: time.Unix(3, 0).UTC(),
			insertFileID: df1.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test4test4"),
			insertValue:  []byte("t4"),
			insertExpiry: time.Unix(4, 0).UTC(),
			insertFileID: df2.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("k00"),
			insertValue:  []byte("v00"),
			insertExpiry: time.Unix(5, 0).UTC(),
			insertFileID: df3.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("k01"),
			insertValue:  []byte("v01"),
			insertExpiry: time.Unix(6, 0).UTC(),
			insertFileID: df3.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("k02"),
			insertValue:  []byte("v02"),
			insertExpiry: time.Unix(7, 0).UTC(),
			insertFileID: df3.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("k03"),
			insertValue:  []byte("v03"),
			insertExpiry: time.Unix(8, 0).UTC(),
			insertFileID: df3.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("k04"),
			insertValue:  []byte("v04"),
			insertExpiry: time.Unix(9, 0).UTC(),
			insertFileID: df3.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test123"),
			insertValue:  []byte("value123"),
			insertExpiry: time.Unix(123, 0).UTC(),
			insertFileID: df1.FileID(),
		},
		{
			isDelete:  true,
			deleteKey: []byte("test1"),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test1234"),
			insertValue:  []byte("value1234"),
			insertExpiry: time.Unix(1234, 0).UTC(),
			insertFileID: df1.FileID(),
		},
		{
			isDelete:  true,
			deleteKey: []byte("test2"),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test12345"),
			insertValue:  []byte("value12345"),
			insertExpiry: time.Unix(12345, 0).UTC(),
			insertFileID: df1.FileID(),
		},
		{
			isDelete:     false,
			insertKey:    []byte("test123456"),
			insertValue:  []byte("value123456"),
			insertExpiry: time.Unix(123456, 0).UTC(),
			insertFileID: df2.FileID(),
		},
	}
	if len(expectSeq) != len(dst.seq) {
		t.Errorf("expect seq:%d, actual seq:%d", len(expectSeq), len(dst.seq))
	}
	for i, s := range dst.seq {
		if expectSeq[i].isDelete != s.isDelete {
			t.Errorf("[%d] isDelete expect:%v actual:%v", i, expectSeq[i].isDelete, s.isDelete)
		}
		if s.isDelete {
			if bytes.Equal(expectSeq[i].deleteKey, s.key) != true {
				t.Errorf("[%d] delete key expect:%s actual:%s", i, expectSeq[i].deleteKey, s.key)
			}
			continue
		}

		if expectSeq[i].insertFileID != s.fileID {
			t.Errorf("[%d] fileID expect:%d actual:%d", i, expectSeq[i].insertFileID, s.fileID)
		}
		if bytes.Equal(expectSeq[i].insertKey, s.payload.Key) != true {
			t.Errorf("[%d] key expect:%s actual:%s", i, expectSeq[i].insertKey, s.payload.Key)
		}
		buf := bytes.NewBuffer(nil)
		buf.ReadFrom(s.payload.Value)
		if bytes.Equal(expectSeq[i].insertValue, buf.Bytes()) != true {
			t.Errorf("[%d] value expect size:%d actual size:%d", i, len(expectSeq[i].insertValue), buf.Len())
		}
		if expectSeq[i].insertExpiry != s.payload.Expiry {
			t.Errorf("[%d] expiry expect:%s actual:%s", i, expectSeq[i].insertExpiry, s.payload.Expiry)
		}
	}
	for _, s := range dst.seq {
		if s.isDelete {
			continue
		}
		s.payload.Close()
	}
}

func TestRepliStreamReciverRepli(t *testing.T) {
	t.Run("emptysource_emptydestination", testRepliStreamReciverRepliEmptySourceEmptyDestination)
	t.Run("behind_small", testRepliStreamReciverRepliDestinationBehindSourceSmallData)
	t.Run("behind_large", testRepliStreamReciverRepliDestinationBehindSourceLargeData)
	t.Run("behind_source_processing", testRepliStreamReciverRepliDestinationBehindSourceProcessing)
}

func TestRepliTemporaryRepliData(t *testing.T) {
	t.Run("close_write", func(tt *testing.T) {
		f, err := openTemporaryRepliData(runtime.DefaultContext(), "")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer f.Remove()

		if err := f.Write(RepliData{
			IsDelete: true,
			EntryKey: []byte("test1"),
		}); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		f.CloseWrite()

		e := f.Write(RepliData{
			IsDelete: true,
			EntryKey: []byte("test2"),
		})

		if errors.Is(e, errWriteClosed) != true {
			tt.Errorf("errWriteClosed! %+v", e)
		}

		data := make([]RepliData, 0)
		f.ReadAll(func(d RepliData) error {
			data = append(data, d)
			return nil
		})

		if len(data) != 1 {
			tt.Errorf("remains test1")
		}
		if data[0].IsDelete != true {
			tt.Errorf("test1 is delete")
		}
		if bytes.Equal(data[0].EntryKey, []byte("test1")) != true {
			tt.Errorf("test1 actual=%s", data[0].EntryKey)
		}
	})
	t.Run("write/read", func(tt *testing.T) {
		f, err := openTemporaryRepliData(runtime.DefaultContext(), "")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		defer f.Remove()

		if err := f.Write(RepliData{
			IsDelete: true,
			EntryKey: []byte("test1"),
		}); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if err := f.Write(RepliData{
			IsDelete:    false,
			FileID:      0,
			Index:       0,
			Size:        123,
			Checksum:    1234567,
			PartKeys:    nil,
			EntryKey:    []byte("test2"),
			EntryValue:  []byte("helloworld"),
			EntryExpiry: time.Time{},
		}); err != nil {
			tt.Errorf("no error: %+v", err)
		}
		if err := f.Write(RepliData{
			IsDelete:    false,
			FileID:      1,
			Index:       1000,
			Size:        12345678,
			Checksum:    101010,
			PartKeys:    []string{"key1", "key2"},
			EntryKey:    []byte("test3"),
			EntryValue:  nil,
			EntryExpiry: time.Time{},
		}); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		data := make([]RepliData, 0)
		if err := f.ReadAll(func(d RepliData) error {
			data = append(data, d)
			return nil
		}); err != nil {
			tt.Errorf("no error: %+v", err)
		}

		if len(data) != 3 {
			tt.Errorf("write 3 data: %d", len(data))
		}
		if data[0].IsDelete != true {
			tt.Errorf("[0] data is delete")
		}
		if bytes.Equal(data[0].EntryKey, []byte("test1")) != true {
			tt.Errorf("[0] key test1 %s", data[0].EntryKey)
		}

		if data[1].IsDelete {
			tt.Errorf("[1] data is not delete")
		}
		if bytes.Equal(data[1].EntryKey, []byte("test2")) != true {
			tt.Errorf("[1] key actual:%s", data[1].EntryKey)
		}
		if data[1].FileID != 0 {
			tt.Errorf("[1] fileID %d", data[1].FileID)
		}
		if data[1].Index != 0 {
			tt.Errorf("[1] index %d", data[1].Index)
		}
		if data[1].Size != 123 {
			tt.Errorf("[1] size %d", data[1].Size)
		}
		if data[1].Checksum != 1234567 {
			tt.Errorf("[1] checksum %d", data[1].Checksum)
		}
		if data[1].PartKeys != nil {
			tt.Errorf("[1] partkey is nil")
		}
		if bytes.Equal(data[1].EntryValue, []byte("helloworld")) != true {
			tt.Errorf("[1] data is helloworld: %s", data[1].EntryValue)
		}

		if data[2].IsDelete {
			tt.Errorf("[2] data is not delete")
		}
		if bytes.Equal(data[2].EntryKey, []byte("test3")) != true {
			tt.Errorf("[2] key actual:%s", data[2].EntryKey)
		}
		if data[2].FileID != 1 {
			tt.Errorf("[2] fileID %d", data[2].FileID)
		}
		if data[2].Index != 1000 {
			tt.Errorf("[2] index %d", data[2].Index)
		}
		if data[2].Size != 12345678 {
			tt.Errorf("[2] size %d", data[2].Size)
		}
		if data[2].Checksum != 101010 {
			tt.Errorf("[2] checksum %d", data[2].Checksum)
		}
		if reflect.DeepEqual(data[2].PartKeys, []string{"key1", "key2"}) != true {
			tt.Errorf("[2] partkey %v", data[2].PartKeys)
		}
		if data[2].EntryValue != nil {
			tt.Errorf("[1] data is nil: %v", data[1].EntryValue)
		}
	})
	t.Run("remove", func(tt *testing.T) {
		f, err := openTemporaryRepliData(runtime.DefaultContext(), "")
		if err != nil {
			tt.Fatalf("no error: %+v", err)
		}
		if err := f.Remove(); err != nil {
			tt.Fatalf("no error: %+v", err)
		}

		e := f.Write(RepliData{
			IsDelete: true,
			EntryKey: []byte("removed"),
		})
		if errors.Is(e, os.ErrClosed) != true {
			tt.Errorf("removed!")
		}
	})
}
