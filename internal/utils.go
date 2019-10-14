package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Exists returns `true` if the given `path` on the current file system exists
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DirSize returns the space occupied by the given `path` on disk on the current
// file system.
func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

// GetDatafiles returns a list of all data files stored in the database path
// given by `path`. All datafiles are identified by the the glob `*.data` and
// the basename is represented by an monotomic increasing integer.
func GetDatafiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(fns)
	return fns, nil
}

// ParseIds will parse a list of datafiles as returned by `GetDatafiles` and
// extract the id part and return a slice of ints.
func ParseIds(fns []string) ([]int, error) {
	var ids []int
	for _, fn := range fns {
		fn = filepath.Base(fn)
		ext := filepath.Ext(fn)
		if ext != ".data" {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 32)
		if err != nil {
			return nil, err
		}
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	return ids, nil
}
