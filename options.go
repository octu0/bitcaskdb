package bitcask

import "github.com/prologic/bitcask/internal/config"

const (
	// DefaultMaxDatafileSize is the default maximum datafile size in bytes
	DefaultMaxDatafileSize = 1 << 20 // 1MB

	// DefaultMaxKeySize is the default maximum key size in bytes
	DefaultMaxKeySize = uint32(64) // 64 bytes

	// DefaultMaxValueSize is the default value size in bytes
	DefaultMaxValueSize = uint64(1 << 16) // 65KB

	// DefaultSync is the default file synchronization action
	DefaultSync = false

	// DefaultAutoRecovery is the default auto-recovery action.
)

// Option is a function that takes a config struct and modifies it
type Option func(*config.Config) error

// WithAutoRecovery sets auto recovery of data and index file recreation.
// IMPORTANT: This flag MUST BE used only if a proper backup was made of all
// the existing datafiles.
func WithAutoRecovery(enabled bool) Option {
	return func(cfg *config.Config) error {
		cfg.AutoRecovery = enabled
		return nil
	}
}

// WithMaxDatafileSize sets the maximum datafile size option
func WithMaxDatafileSize(size int) Option {
	return func(cfg *config.Config) error {
		cfg.MaxDatafileSize = size
		return nil
	}
}

// WithMaxKeySize sets the maximum key size option
func WithMaxKeySize(size uint32) Option {
	return func(cfg *config.Config) error {
		cfg.MaxKeySize = size
		return nil
	}
}

// WithMaxValueSize sets the maximum value size option
func WithMaxValueSize(size uint64) Option {
	return func(cfg *config.Config) error {
		cfg.MaxValueSize = size
		return nil
	}
}

// WithSync causes Sync() to be called on every key/value written increasing
// durability and safety at the expense of performance
func WithSync(sync bool) Option {
	return func(cfg *config.Config) error {
		cfg.Sync = sync
		return nil
	}
}

func newDefaultConfig() *config.Config {
	return &config.Config{
		MaxDatafileSize: DefaultMaxDatafileSize,
		MaxKeySize:      DefaultMaxKeySize,
		MaxValueSize:    DefaultMaxValueSize,
		Sync:            DefaultSync,
	}
}
