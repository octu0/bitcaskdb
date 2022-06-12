package datafile

import (
	"os"

	"github.com/octu0/bitcaskdb/context"
)

type datafileOptFunc func(*datafileOpt)

type datafileOpt struct {
	ctx                    *context.Context
	path                   string
	fileID                 int32
	readonly               bool
	fileMode               os.FileMode
	tempDir                string
	copyTempThreshold      int64
	valueOnMemoryThreshold int64
}

func Context(ctx *context.Context) datafileOptFunc {
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

func ValueOnMemoryThreshold(threshold int64) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.valueOnMemoryThreshold = threshold
	}
}

func readonly(enable bool) datafileOptFunc {
	return func(opt *datafileOpt) {
		opt.readonly = enable
	}
}
