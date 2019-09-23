package codec

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/prologic/bitcask/internal"
	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	_, err := encoder.Encode(internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
		Offset:   424242,
	})

	expectedHex := "0000000500000000000000076d796b65796d7976616c7565000651bd"
	if assert.NoError(err) {
		assert.Equal(expectedHex, hex.EncodeToString(buf.Bytes()))
	}
}
