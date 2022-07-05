package datafile

import (
	"os"

	"github.com/octu0/bitcaskdb/runtime"
)

type datafileOptFunc func(*datafileOpt)

type datafileOpt struct {
	ctx               runtime.Context
	readonly          bool
	fileMode          os.FileMode
	tempDir           string
	copyTempThreshold int64
}

func RuntimeContext(ctx runtime.Context) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.ctx = ctx
	}
}

func FileMode(mode os.FileMode) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.fileMode = mode
	}
}

func TempDir(dir string) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.tempDir = dir
	}
}

func CopyTempThreshold(threshold int64) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.copyTempThreshold = threshold
	}
}

func readonly(enable bool) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.readonly = enable
	}
}

func initDatafileOpt(opt *datafileOpt) {
	if opt.ctx == nil {
		opt.ctx = runtime.DefaultContext()
	}
	if opt.fileMode == 0 {
		opt.fileMode = os.FileMode(0600)
	}
}
