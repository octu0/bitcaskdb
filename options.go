package bitcask

const (
	DefaultMaxDatafileSize = 1 << 20 // 1MB
	DefaultMaxKeySize      = 64      // 64 bytes
	DefaultMaxValueSize    = 1 << 16 // 65KB
)

// Option ...
type Option option

type option func(*config) error

type config struct {
	MaxDatafileSize int
	MaxKeySize      int
	MaxValueSize    int
}

func newDefaultConfig() *config {
	return &config{
		MaxDatafileSize: DefaultMaxDatafileSize,
		MaxKeySize:      DefaultMaxKeySize,
		MaxValueSize:    DefaultMaxValueSize,
	}
}

func WithMaxDatafileSize(size int) option {
	return func(cfg *config) error {
		cfg.MaxDatafileSize = size
		return nil
	}
}

func WithMaxKeySize(size int) option {
	return func(cfg *config) error {
		cfg.MaxKeySize = size
		return nil
	}
}

func WithMaxValueSize(size int) option {
	return func(cfg *config) error {
		cfg.MaxValueSize = size
		return nil
	}
}
