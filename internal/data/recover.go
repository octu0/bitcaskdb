package data

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"git.mills.io/prologic/bitcask/internal"
	"git.mills.io/prologic/bitcask/internal/config"
	"git.mills.io/prologic/bitcask/internal/data/codec"
)

// CheckAndRecover checks and recovers the last datafile.
// If the datafile isn't corrupted, this is a noop. If it is,
// the longest non-corrupted prefix will be kept and the rest
// will be *deleted*. Also, the index file is also *deleted* which
// will be automatically recreated on next startup.
func CheckAndRecover(path string, cfg *config.Config) error {
	dfs, err := internal.GetDatafiles(path)
	if err != nil {
		return fmt.Errorf("scanning datafiles: %s", err)
	}
	if len(dfs) == 0 {
		return nil
	}
	f := dfs[len(dfs)-1]
	recovered, err := recoverDatafile(f, cfg)
	if err != nil {
		return fmt.Errorf("error recovering data file: %s", err)
	}
	if recovered {
		if err := os.Remove(filepath.Join(path, "index")); err != nil {
			return fmt.Errorf("error deleting the index on recovery: %s", err)
		}
	}
	return nil
}

func recoverDatafile(path string, cfg *config.Config) (recovered bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("opening the datafile: %s", err)
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
		return false, fmt.Errorf("creating the recovered datafile: %w", err)
	}
	defer func() {
		closeErr := fr.Close()
		if err == nil {
			err = closeErr
		}
	}()

	dec := codec.NewDecoder(f, cfg.MaxKeySize, cfg.MaxValueSize)
	enc := codec.NewEncoder(fr)
	e := internal.Entry{}

	corrupted := false
	for !corrupted {
		_, err = dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if codec.IsCorruptedData(err) {
			corrupted = true
			continue
		}
		if err != nil {
			return false, fmt.Errorf("unexpected error while reading datafile: %w", err)
		}
		if _, err := enc.Encode(e); err != nil {
			return false, fmt.Errorf("writing to recovered datafile: %w", err)
		}
	}
	if !corrupted {
		if err := os.Remove(fr.Name()); err != nil {
			return false, fmt.Errorf("can't remove temporal recovered datafile: %w", err)
		}
		return false, nil
	}
	if err := os.Rename(rPath, path); err != nil {
		return false, fmt.Errorf("removing corrupted file: %s", err)
	}
	return true, nil
}
