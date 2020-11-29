package metadata

import (
	"os"

	"github.com/prologic/bitcask/internal"
)

type MetaData struct {
	IndexUpToDate bool `json:"index_up_to_date"`
}

func (m *MetaData) Save(path string, mode os.FileMode) error {
	return internal.SaveJsonToFile(m, path, mode)
}

func Load(path string) (*MetaData, error) {
	var m MetaData
	err := internal.LoadFromJsonFile(path, &m)
	return &m, err
}
