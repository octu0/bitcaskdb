package runtime

import (
	"github.com/octu0/bp"
)

var (
	_ Buffer = (*defaultBuffer)(nil)
)

const (
	defaultBufferSize int = 128 * 1024
)

type Buffer interface {
	BufferSize() int
	BytePool() *bp.BytePool
	BufferPool() *bp.BufferPool
	BufioReaderPool() *bp.BufioReaderPool
	BufioWriterPool() *bp.BufioWriterPool
}

type defaultBuffer struct {
	bufferSize      int
	bytePool        *bp.BytePool
	bufferPool      *bp.BufferPool
	bufioReaderPool *bp.BufioReaderPool
	bufioWriterPool *bp.BufioWriterPool
}

func (b *defaultBuffer) BufferSize() int {
	return b.bufferSize
}

func (b *defaultBuffer) BytePool() *bp.BytePool {
	return b.bytePool
}

func (b *defaultBuffer) BufferPool() *bp.BufferPool {
	return b.bufferPool
}

func (b *defaultBuffer) BufioReaderPool() *bp.BufioReaderPool {
	return b.bufioReaderPool
}

func (b *defaultBuffer) BufioWriterPool() *bp.BufioWriterPool {
	return b.bufioWriterPool
}

func createDefaultBuffer() *defaultBuffer {
	return &defaultBuffer{
		bufferSize:      defaultBufferSize,
		bytePool:        bp.NewBytePool(1000, defaultBufferSize),
		bufferPool:      bp.NewBufferPool(1000, defaultBufferSize),
		bufioReaderPool: bp.NewBufioReaderSizePool(1000, defaultBufferSize),
		bufioWriterPool: bp.NewBufioWriterSizePool(1000, defaultBufferSize),
	}
}
