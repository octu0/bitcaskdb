package util

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
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
