package bitcaskdb

import (
	"encoding/json"
	"os"
)

const (
	// DefaultDirFileModeBeforeUmask is the default os.FileMode used when creating directories
	DefaultDirFileModeBeforeUmask = os.FileMode(0700)

	// DefaultFileFileModeBeforeUmask is the default os.FileMode used when creating files
	DefaultFileFileModeBeforeUmask = os.FileMode(0600)

	// DefaultMaxDatafileSize is the default maximum datafile size in bytes
	DefaultMaxDatafileSize = 1 << 20 // 1MB

	// Data size exceeding this threshold to temporarily copied to TempDir
	DefaultCopyTempThrshold int64 = 10 * 1024 * 1024

	// DefaultSync is the default file synchronization action
	DefaultSync = false

	CurrentDBVersion = uint32(1)
)

// Option is a function that takes a config struct and modifies it
type Option func(*Config) error

// Config contains the bitcask configuration parameters
type Config struct {
	MaxDatafileSize         int    `json:"max_datafile_size"`
	Sync                    bool   `json:"sync"`
	AutoRecovery            bool   `json:"autorecovery"`
	DBVersion               uint32 `json:"db_version"`
	ValidateChecksum        bool
	TempDir                 string
	CopyTempThreshold       int64
	DirFileModeBeforeUmask  os.FileMode
	FileFileModeBeforeUmask os.FileMode
}

func withConfig(src *Config) Option {
	return func(cfg *Config) error {
		cfg.MaxDatafileSize = src.MaxDatafileSize
		cfg.Sync = src.Sync
		cfg.AutoRecovery = src.AutoRecovery
		cfg.ValidateChecksum = src.ValidateChecksum
		cfg.DirFileModeBeforeUmask = src.DirFileModeBeforeUmask
		cfg.FileFileModeBeforeUmask = src.FileFileModeBeforeUmask
		return nil
	}
}

// WithAutoRecovery sets auto recovery of data and index file recreation.
// IMPORTANT: This flag MUST BE used only if a proper backup was made of all
// the existing datafiles.
func WithAutoRecovery(enabled bool) Option {
	return func(cfg *Config) error {
		cfg.AutoRecovery = enabled
		return nil
	}
}

// WithDirFileModeBeforeUmask sets the FileMode used for each new file created.
func WithDirFileModeBeforeUmask(mode os.FileMode) Option {
	return func(cfg *Config) error {
		cfg.DirFileModeBeforeUmask = mode
		return nil
	}
}

// WithFileFileModeBeforeUmask sets the FileMode used for each new file created.
func WithFileFileModeBeforeUmask(mode os.FileMode) Option {
	return func(cfg *Config) error {
		cfg.FileFileModeBeforeUmask = mode
		return nil
	}
}

// WithMaxDatafileSize sets the maximum datafile size option
func WithMaxDatafileSize(size int) Option {
	return func(cfg *Config) error {
		cfg.MaxDatafileSize = size
		return nil
	}
}

// WithSync causes Sync() to be called on every key/value written increasing
// durability and safety at the expense of performance
func WithSync(sync bool) Option {
	return func(cfg *Config) error {
		cfg.Sync = sync
		return nil
	}
}

func WithValidateChecksum(enable bool) Option {
	return func(cfg *Config) error {
		cfg.ValidateChecksum = enable
		return nil
	}
}

func WithTempDir(dir string) Option {
	return func(cfg *Config) error {
		if dir == "" {
			cfg.TempDir = os.TempDir()
		} else {
			cfg.TempDir = dir
		}
		return nil
	}
}

func WithCopyTempThreshold(size int64) Option {
	return func(cfg *Config) error {
		cfg.CopyTempThreshold = size
		return nil
	}
}

func newDefaultConfig() *Config {
	return &Config{
		MaxDatafileSize:         DefaultMaxDatafileSize,
		Sync:                    DefaultSync,
		ValidateChecksum:        false,
		TempDir:                 os.TempDir(),
		CopyTempThreshold:       DefaultCopyTempThrshold,
		DirFileModeBeforeUmask:  DefaultDirFileModeBeforeUmask,
		FileFileModeBeforeUmask: DefaultFileFileModeBeforeUmask,
		DBVersion:               CurrentDBVersion,
	}
}

// Load loads a configuration from the given path
func ConfigLoad(path string) (*Config, error) {
	var cfg Config

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Save saves the configuration to the provided path
func ConfigSave(path string, c *Config) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}

	if err = os.WriteFile(path, data, c.FileFileModeBeforeUmask); err != nil {
		return err
	}

	return nil
}
