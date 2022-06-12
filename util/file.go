package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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
// the basename is represented by a monotonic increasing integer.
// The returned files are *sorted* in increasing order.
func GetDatafiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(fns)
	return fns, nil
}

// Copy copies source contents to destination
func CopyFiles(dst, src string, exclude []string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		relPath := strings.Replace(path, src, "", 1)
		if relPath == "" {
			return nil
		}
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			return os.Mkdir(filepath.Join(dst, relPath), info.Mode())
		}
		var data, err1 = ioutil.ReadFile(filepath.Join(src, relPath))
		if err1 != nil {
			return err1
		}
		return ioutil.WriteFile(filepath.Join(dst, relPath), data, info.Mode())
	})
}
