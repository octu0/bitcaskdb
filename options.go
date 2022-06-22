package bitcaskdb

import (
	"log"
	"os"
	"time"

	"github.com/octu0/bitcaskdb/runtime"
)

const (
	// DefaultDirFileModeBeforeUmask is the default os.FileMode used when creating directories
	DefaultDirFileModeBeforeUmask = os.FileMode(0700)

	// DefaultFileFileModeBeforeUmask is the default os.FileMode used when creating files
	DefaultFileFileModeBeforeUmask = os.FileMode(0600)

	// DefaultMaxDatafileSize is the default maximum datafile size in bytes
	DefaultMaxDatafileSize = 100 * 1024 * 1024 // 100MB

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
)

// Option is a function that takes a option struct and modifies it
type OptionFunc func(*option) error

// option contains the bitcask configuration parameters
type option struct {
	RuntimeContext          runtime.Context
	MaxDatafileSize         int
	Sync                    bool
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

func withMerge(src *option) OptionFunc {
	return func(opt *option) error {
		opt.RuntimeContext = src.RuntimeContext
		opt.MaxDatafileSize = src.MaxDatafileSize
		opt.Sync = src.Sync
		opt.ValidateChecksum = src.ValidateChecksum
		opt.Logger = src.Logger
		opt.TempDir = src.TempDir
		opt.CopyTempThreshold = src.CopyTempThreshold
		opt.ValueOnMemoryThreshold = src.ValueOnMemoryThreshold
		opt.DirFileModeBeforeUmask = src.DirFileModeBeforeUmask
		opt.FileFileModeBeforeUmask = src.FileFileModeBeforeUmask
		opt.RepliBindIP = src.RepliBindIP
		opt.RepliBindPort = src.RepliBindPort
		opt.RepliServerIP = src.RepliServerIP
		opt.RepliServerPort = src.RepliServerPort
		opt.RepliRequestTimeout = src.RepliRequestTimeout
		opt.NoRepliEmit = true
		opt.NoRepliRecv = true
		return nil
	}
}

// WithDirFileModeBeforeUmask sets the FileMode used for each new file created.
func WithDirFileModeBeforeUmask(mode os.FileMode) OptionFunc {
	return func(opt *option) error {
		opt.DirFileModeBeforeUmask = mode
		return nil
	}
}

// WithFileFileModeBeforeUmask sets the FileMode used for each new file created.
func WithFileFileModeBeforeUmask(mode os.FileMode) OptionFunc {
	return func(opt *option) error {
		opt.FileFileModeBeforeUmask = mode
		return nil
	}
}

// WithMaxDatafileSize sets the maximum datafile size option
func WithMaxDatafileSize(size int) OptionFunc {
	return func(opt *option) error {
		opt.MaxDatafileSize = size
		return nil
	}
}

// WithSync causes Sync() to be called on every key/value written increasing
// durability and safety at the expense of performance
func WithSync(sync bool) OptionFunc {
	return func(opt *option) error {
		opt.Sync = sync
		return nil
	}
}

func WithRepli(bindIP string, bindPort int) OptionFunc {
	return func(opt *option) error {
		opt.NoRepliEmit = false
		opt.RepliBindIP = bindIP
		opt.RepliBindPort = bindPort
		return nil
	}
}

func WithRepliClient(serverIP string, serverPort int) OptionFunc {
	return func(opt *option) error {
		opt.NoRepliRecv = false
		opt.RepliServerIP = serverIP
		opt.RepliServerPort = serverPort
		return nil
	}
}

func WithRepliClientRequestTimeout(rto time.Duration) OptionFunc {
	return func(opt *option) error {
		opt.RepliRequestTimeout = rto
		return nil
	}
}

func WithValidateChecksum(enable bool) OptionFunc {
	return func(opt *option) error {
		opt.ValidateChecksum = enable
		return nil
	}
}

func WithLogger(logger *log.Logger) OptionFunc {
	return func(opt *option) error {
		opt.Logger = logger
		return nil
	}
}

func WithTempDir(dir string) OptionFunc {
	return func(opt *option) error {
		if dir == "" {
			opt.TempDir = os.TempDir()
		} else {
			opt.TempDir = dir
		}
		return nil
	}
}

func WithCopyTempThreshold(size int64) OptionFunc {
	return func(opt *option) error {
		opt.CopyTempThreshold = size
		return nil
	}
}

func WithValueOnMemoryThreshold(size int64) OptionFunc {
	return func(opt *option) error {
		opt.ValueOnMemoryThreshold = size
		return nil
	}
}

func WithRuntimeContext(ctx runtime.Context) OptionFunc {
	return func(opt *option) error {
		opt.RuntimeContext = ctx
		return nil
	}
}

func newDefaultOption() *option {
	return &option{
		RuntimeContext:          runtime.DefaultContext(),
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
	}
}
