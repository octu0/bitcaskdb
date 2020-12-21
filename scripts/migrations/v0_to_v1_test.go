package migrations

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ApplyV0ToV1(t *testing.T) {
	assert := assert.New(t)
	testdir, err := ioutil.TempDir("/tmp", "bitcask")
	assert.NoError(err)
	defer os.RemoveAll(testdir)
	w0, err := os.OpenFile(filepath.Join(testdir, "000000000.data"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	assert.NoError(err)
	w1, err := os.OpenFile(filepath.Join(testdir, "000000001.data"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	assert.NoError(err)
	w2, err := os.OpenFile(filepath.Join(testdir, "000000002.data"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	assert.NoError(err)
	defer w0.Close()
	defer w1.Close()
	defer w2.Close()
	buf := make([]byte, 104)
	binary.BigEndian.PutUint32(buf[:4], 5)
	binary.BigEndian.PutUint64(buf[4:12], 7)
	copy(buf[12:28], "mykeymyvalue0AAA")
	binary.BigEndian.PutUint32(buf[28:32], 3)
	binary.BigEndian.PutUint64(buf[32:40], 5)
	copy(buf[40:52], "keyvalue0BBB")
	_, err = w0.Write(buf[:52])
	assert.NoError(err)
	_, err = w1.Write(buf[:52])
	assert.NoError(err)
	_, err = w2.Write(buf[:52])
	assert.NoError(err)
	err = ApplyV0ToV1(testdir, 104)
	assert.NoError(err)
	r0, err := os.Open(filepath.Join(testdir, "000000000.data"))
	assert.NoError(err)
	defer r0.Close()
	n, err := io.ReadFull(r0, buf)
	assert.NoError(err)
	assert.Equal(104, n)
	assert.Equal("0000000500000000000000076d796b65796d7976616c75653041414100000000000000000000000300000000000000056b657976616c75653042424200000000000000000000000500000000000000076d796b65796d7976616c7565304141410000000000000000", hex.EncodeToString(buf))
	r1, err := os.Open(filepath.Join(testdir, "000000001.data"))
	assert.NoError(err)
	defer r1.Close()
	n, err = io.ReadFull(r1, buf[:100])
	assert.NoError(err)
	assert.Equal(100, n)
	assert.Equal("0000000300000000000000056b657976616c75653042424200000000000000000000000500000000000000076d796b65796d7976616c75653041414100000000000000000000000300000000000000056b657976616c7565304242420000000000000000", hex.EncodeToString(buf[:100]))
}
