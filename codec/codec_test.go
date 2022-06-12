package codec

import (
	"testing"
)

func TestPayload(t *testing.T) {
	t.Run("Close", func(tt *testing.T) {
		p := &Payload{}
		p.Close()
	})
}
