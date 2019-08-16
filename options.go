package bitcask

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

const (
	// DefaultMaxDatafileSize is the default maximum datafile size in bytes
	DefaultMaxDatafileSize = 1 << 20 // 1MB

	// DefaultMaxKeySize is the default maximum key size in bytes
	DefaultMaxKeySize = 64 // 64 bytes

	// DefaultMaxValueSize is the default value size in bytes
	DefaultMaxValueSize = 1 << 16 // 65KB
)

// Option is a function that takes a config struct and modifies it
type Option func(*config) error

type config struct {
	maxDatafileSize int
	maxKeySize      int
	maxValueSize    int
	sync            bool
	greedyScan      bool
}

func (c *config) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MaxDatafileSize int  `json:"max_datafile_size"`
		MaxKeySize      int  `json:"max_key_size"`
		MaxValueSize    int  `json:"max_value_size"`
		Sync            bool `json:"sync"`
		GreedyScan      bool `json:"greedy_scan"`
	}{
		MaxDatafileSize: c.maxDatafileSize,
		MaxKeySize:      c.maxKeySize,
		MaxValueSize:    c.maxValueSize,
		Sync:            c.sync,
		GreedyScan:      c.greedyScan,
	})
}

func getConfig(path string) (*config, error) {
	type Config struct {
		MaxDatafileSize int  `json:"max_datafile_size"`
		MaxKeySize      int  `json:"max_key_size"`
		MaxValueSize    int  `json:"max_value_size"`
		Sync            bool `json:"sync"`
		GreedyScan      bool `json:"greedy_scan"`
	}

	var cfg Config

	data, err := ioutil.ReadFile(filepath.Join(path, "config.json"))
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &config{
		maxDatafileSize: cfg.MaxDatafileSize,
		maxKeySize:      cfg.MaxKeySize,
		maxValueSize:    cfg.MaxValueSize,
		sync:            cfg.Sync,
		greedyScan:      cfg.GreedyScan,
	}, nil
}

func newDefaultConfig() *config {
	return &config{
		maxDatafileSize: DefaultMaxDatafileSize,
		maxKeySize:      DefaultMaxKeySize,
		maxValueSize:    DefaultMaxValueSize,
	}
}

// WithMaxDatafileSize sets the maximum datafile size option
func WithMaxDatafileSize(size int) Option {
	return func(cfg *config) error {
		cfg.maxDatafileSize = size
		return nil
	}
}

// WithMaxKeySize sets the maximum key size option
func WithMaxKeySize(size int) Option {
	return func(cfg *config) error {
		cfg.maxKeySize = size
		return nil
	}
}

// WithMaxValueSize sets the maximum value size option
func WithMaxValueSize(size int) Option {
	return func(cfg *config) error {
		cfg.maxValueSize = size
		return nil
	}
}

// WithSync causes Sync() to be called on every key/value written increasing
// durability and safety at the expense of performance
func WithSync(sync bool) Option {
	return func(cfg *config) error {
		cfg.sync = sync
		return nil
	}
}

// WithGreedyScan enables faster Scan performance.
// This is disabled by default because it causes high memory usage.
func WithGreedyScan(enabled bool) Option {
	return func(cfg *config) error {
		cfg.greedyScan = enabled
		return nil
	}
}
