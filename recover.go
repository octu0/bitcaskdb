package bitcaskdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/octu0/bitcaskdb/codec"
	"github.com/octu0/bitcaskdb/context"
	"github.com/octu0/bitcaskdb/util"
)

// CheckAndRecover checks and recovers the last datafile.
// If the datafile isn't corrupted, this is a noop. If it is,
// the longest non-corrupted prefix will be kept and the rest
// will be *deleted*. Also, the index file is also *deleted* which
// will be automatically recreated on next startup.
func CheckAndRecover(ctx *context.Context, path string, cfg *Config) error {
	dfs, err := util.GetDatafiles(path)
	if err != nil {
		return fmt.Errorf("scanning datafiles: %s", err)
	}
	if len(dfs) == 0 {
		return nil
	}
	f := dfs[len(dfs)-1]
	recovered, err := recoverDatafile(ctx, f, cfg)
	if err != nil {
		return errors.Wrapf(err, "error recovering data file: %s", f)
	}
	if recovered {
		if err := os.Remove(filepath.Join(path, filerIndexFile)); err != nil {
			return errors.Wrap(err, "error deleting the index on recovery")
		}
	}
	return nil
}

func recoverDatafile(ctx *context.Context, path string, cfg *Config) (recovered bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return false, errors.Wrapf(err, "opening the datafile: %s", path)
	}
	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()
	dir, file := filepath.Split(path)
	rPath := filepath.Join(dir, fmt.Sprintf("%s.recovered", file))
	fr, err := os.OpenFile(rPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return false, errors.Wrapf(err, "creating the recovered datafile: %s", rPath)
	}
	defer func() {
		closeErr := fr.Close()
		if err == nil {
			err = closeErr
		}
	}()

	dec := codec.NewDecoder(ctx, f, cfg.ValueOnMemoryThreshold)
	defer dec.Close()

	enc := codec.NewEncoder(ctx, fr, cfg.TempDir, cfg.CopyTempThreshold)
	defer enc.Close()

	corrupted := false
	for !corrupted {
		p, err := dec.Decode()
		if errors.Is(err, io.EOF) {
			break
		}
		if p != nil {
			defer p.Close()
		}

		if codec.IsCorruptedData(err) {
			corrupted = true
			continue
		}
		if err != nil {
			return false, errors.Wrap(err, "unexpected error while reading datafile")
		}
		if _, err := enc.Encode(p.Key, p.Value, p.Expiry); err != nil {
			return false, errors.Wrap(err, "writing to recovered datafile")
		}
	}
	if corrupted != true {
		if err := os.Remove(fr.Name()); err != nil {
			return false, errors.Wrapf(err, "can't remove temporal recovered datafile: %s", fr.Name())
		}
		return false, nil
	}
	if err := os.Rename(rPath, path); err != nil {
		return false, errors.Wrapf(err, "removing corrupted file: %s %s", rPath, path)
	}
	return true, nil
}
