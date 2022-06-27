package datafile

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const (
	defaultDatafileFilename string = "%09d.data"
)

// GetDatafiles returns a list of all data files stored in the database path
// given by `path`. All datafiles are identified by the the glob `*.data` and
// the basename is represented by a monotonic increasing integer.
// The returned files are *sorted* in increasing order.
func getDatafiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sort.Strings(fns)
	return fns, nil
}

// Exists returns `true` if the given `path` on the current file system exists
// ParseIds will parse a list of datafiles as returned by `GetDatafiles` and
// extract the id part and return a slice of ints.
func ParseIds(fns []string) ([]int32, error) {
	ids := make([]int32, 0, len(fns))
	for _, fn := range fns {
		fn = filepath.Base(fn)
		ext := filepath.Ext(fn)
		if ext != ".data" {
			continue
		}
		// skip hidden file
		if 0 < len(fn) && fn[0] == '.' {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 32)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ids = append(ids, int32(id))
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}

func ParseIdsFromDatafiles(path string) ([]int32, error) {
	fns, err := getDatafiles(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ParseIds(fns)
}
