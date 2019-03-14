package bitcask

const (
	DefaultMaxDatafileSize = 1 << 20 // 1MB
	DefaultMaxKeySize      = 64      // 64 bytes
	DefaultMaxValueSize    = 1 << 16 // 65KB
)

type Options struct {
	MaxDatafileSize int
	MaxKeySize      int
	MaxValueSize    int
}

func NewDefaultOptions() Options {
	return Options{
		MaxDatafileSize: DefaultMaxDatafileSize,
		MaxKeySize:      DefaultMaxKeySize,
		MaxValueSize:    DefaultMaxValueSize,
	}
}

func WithMaxDatafileSize(size int) func(*Bitcask) error {
	return func(b *Bitcask) error {
		b.opts.MaxDatafileSize = size
		return nil
	}
}

func WithMaxKeySize(size int) func(*Bitcask) error {
	return func(b *Bitcask) error {
		b.opts.MaxKeySize = size
		return nil
	}
}

func WithMaxValueSize(size int) func(*Bitcask) error {
	return func(b *Bitcask) error {
		b.opts.MaxValueSize = size
		return nil
	}
}
