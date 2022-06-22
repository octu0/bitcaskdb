package datafile

import (
	"os"

	"github.com/octu0/bitcaskdb/runtime"
)

type datafileOptFunc func(*datafileOpt)

type datafileOpt struct {
	ctx               runtime.Context
	path              string
	fileID            int32
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

func Path(path string) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.path = path
	}
}

func FileID(id int32) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.fileID = id
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
