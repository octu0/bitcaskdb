package codec

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"git.mills.io/prologic/bitcask/internal"
	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	var buf bytes.Buffer
	mockTime := time.Date(2020, 10, 1, 0, 0, 0, 0, time.UTC)
	encoder := NewEncoder(&buf)
	_, err := encoder.Encode(internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
		Offset:   424242,
		Expiry:   &mockTime,
	})

	expectedHex := "0000000500000000000000076d796b65796d7976616c7565000651bd000000005f751c00"
	if assert.NoError(err) {
		assert.Equal(expectedHex, hex.EncodeToString(buf.Bytes()))
	}
}
