package util

import (
	"encoding/json"
	"os"
)

// SaveJsonToFile converts v into json and store in file identified by path
func SaveJsonToFile(v interface{}, path string, mode os.FileMode) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, mode)
}

// LoadFromJsonFile reads file located at `path` and put its content in json format in v
func LoadFromJsonFile(path string, v interface{}) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
