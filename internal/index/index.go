package index

import (
	"os"
	"path"

	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/prologic/bitcask/internal"
)

// ReadFromFile reads an index from a persisted file
func ReadFromFile(filePath string, maxKeySize int) (art.Tree, bool, error) {
	t := art.New()
	if !internal.Exists(path.Join(filePath, "index")) {
		return t, false, nil
	}

	f, err := os.Open(path.Join(filePath, "index"))
	if err != nil {
		return t, true, err
	}
	defer f.Close()
	if err := readIndex(f, t, maxKeySize); err != nil {
		return t, true, err
	}
	return t, true, nil
}
