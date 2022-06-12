package util

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

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
		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 32)
		if err != nil {
			return nil, err
		}
		ids = append(ids, int32(id))
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, nil
}
