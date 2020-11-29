package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

// Copy copies source contents to destination
func Copy(src, dst string, exclude []string) error {
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

// SaveJsonToFile converts v into json and store in file identified by path
func SaveJsonToFile(v interface{}, path string, mode os.FileMode) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, b, mode)
}

// LoadFromJsonFile reads file located at `path` and put its content in json format in v
func LoadFromJsonFile(path string, v interface{}) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
