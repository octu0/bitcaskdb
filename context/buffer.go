package context

import (
	"github.com/octu0/bp"
)

const (
	defaultBufferSize int = 128 * 1024
)

type Buffer struct {
	bufferSize      int
	bytePool        *bp.BytePool
	bufferPool      *bp.BufferPool
	bufioReaderPool *bp.BufioReaderPool
	bufioWriterPool *bp.BufioWriterPool
}

func (b *Buffer) BufferSize() int {
	return b.bufferSize
}

func (b *Buffer) BytePool() *bp.BytePool {
	return b.bytePool
}

func (b *Buffer) BufferPool() *bp.BufferPool {
	return b.bufferPool
}

func (b *Buffer) BufioReaderPool() *bp.BufioReaderPool {
	return b.bufioReaderPool
}

func (b *Buffer) BufioWriterPool() *bp.BufioWriterPool {
	return b.bufioWriterPool
}

func createDefaultBuffer() *Buffer {
	return &Buffer{
		bufferSize:      defaultBufferSize,
		bytePool:        bp.NewBytePool(1000, defaultBufferSize),
		bufferPool:      bp.NewBufferPool(1000, defaultBufferSize),
		bufioReaderPool: bp.NewBufioReaderSizePool(1000, defaultBufferSize),
		bufioWriterPool: bp.NewBufioWriterSizePool(1000, defaultBufferSize),
	}
}
