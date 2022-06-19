package bitcaskdb

import (
	"encoding/json"
	"log"
	"os"
	"time"
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

	// Data size larger than this size will be placed on RAM
	DefaultValueOnMemoryThreshold int64 = 128 * 1024

	// DefaultSync is the default file synchronization action
	DefaultSync = false

	DefaultNoRepliEmit         bool          = true
	DefaultRepliBindIP         string        = "[0.0.0.0]"
	DefaultRepliBindPort       int           = 4220
	DefaultNoRepliRecv         bool          = true
	DefaultRepliServerIP       string        = "127.0.0.1"
	DefaultRepliServerPort     int           = 4220
	DefaultRepliRequestTimeout time.Duration = 10 * time.Second

	CurrentDBVersion = uint32(1)
)

// Option is a function that takes a config struct and modifies it
type Option func(*Config) error

// Config contains the bitcask configuration parameters
type Config struct {
	MaxDatafileSize         int    `json:"max_datafile_size"`
	Sync                    bool   `json:"sync"`
	DBVersion               uint32 `json:"db_version"`
	ValidateChecksum        bool
	Logger                  *log.Logger
	TempDir                 string
	CopyTempThreshold       int64
	ValueOnMemoryThreshold  int64
	NoRepliEmit             bool
	RepliBindIP             string
	RepliBindPort           int
	NoRepliRecv             bool
	RepliServerIP           string
	RepliServerPort         int
	RepliRequestTimeout     time.Duration
	DirFileModeBeforeUmask  os.FileMode
	FileFileModeBeforeUmask os.FileMode
}

func withMerge(src *Config) Option {
	return func(cfg *Config) error {
		cfg.MaxDatafileSize = src.MaxDatafileSize
		cfg.Sync = src.Sync
		cfg.ValidateChecksum = src.ValidateChecksum
		cfg.Logger = src.Logger
		cfg.TempDir = src.TempDir
		cfg.CopyTempThreshold = src.CopyTempThreshold
		cfg.ValueOnMemoryThreshold = src.ValueOnMemoryThreshold
		cfg.DirFileModeBeforeUmask = src.DirFileModeBeforeUmask
		cfg.FileFileModeBeforeUmask = src.FileFileModeBeforeUmask
		cfg.RepliBindIP = src.RepliBindIP
		cfg.RepliBindPort = src.RepliBindPort
		cfg.RepliServerIP = src.RepliServerIP
		cfg.RepliServerPort = src.RepliServerPort
		cfg.RepliRequestTimeout = src.RepliRequestTimeout
		cfg.NoRepliEmit = true
		cfg.NoRepliRecv = true
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

func WithRepli(bindIP string, bindPort int) Option {
	return func(cfg *Config) error {
		cfg.NoRepliEmit = false
		cfg.RepliBindIP = bindIP
		cfg.RepliBindPort = bindPort
		return nil
	}
}

func WithRepliClient(serverIP string, serverPort int) Option {
	return func(cfg *Config) error {
		cfg.NoRepliRecv = false
		cfg.RepliServerIP = serverIP
		cfg.RepliServerPort = serverPort
		return nil
	}
}

func WithRepliClientRequestTimeout(rto time.Duration) Option {
	return func(cfg *Config) error {
		cfg.RepliRequestTimeout = rto
		return nil
	}
}

func WithValidateChecksum(enable bool) Option {
	return func(cfg *Config) error {
		cfg.ValidateChecksum = enable
		return nil
	}
}

func WithLogger(logger *log.Logger) Option {
	return func(cfg *Config) error {
		cfg.Logger = logger
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

func WithValueOnMemoryThreshold(size int64) Option {
	return func(cfg *Config) error {
		cfg.ValueOnMemoryThreshold = size
		return nil
	}
}

func newDefaultConfig() *Config {
	return &Config{
		MaxDatafileSize:         DefaultMaxDatafileSize,
		Sync:                    DefaultSync,
		ValidateChecksum:        false,
		Logger:                  log.Default(),
		TempDir:                 os.TempDir(),
		CopyTempThreshold:       DefaultCopyTempThrshold,
		ValueOnMemoryThreshold:  DefaultValueOnMemoryThreshold,
		DirFileModeBeforeUmask:  DefaultDirFileModeBeforeUmask,
		FileFileModeBeforeUmask: DefaultFileFileModeBeforeUmask,
		NoRepliEmit:             DefaultNoRepliEmit,
		RepliBindIP:             DefaultRepliBindIP,
		RepliBindPort:           DefaultRepliBindPort,
		NoRepliRecv:             DefaultNoRepliRecv,
		RepliServerIP:           DefaultRepliServerIP,
		RepliServerPort:         DefaultRepliServerPort,
		RepliRequestTimeout:     DefaultRepliRequestTimeout,
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
