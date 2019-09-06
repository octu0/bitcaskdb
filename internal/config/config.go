package config

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

// Config contains the bitcask configuration parameters
type Config struct {
	MaxDatafileSize int  `json:"max_datafile_size"`
	MaxKeySize      int  `json:"max_key_size"`
	MaxValueSize    int  `json:"max_value_size"`
	Sync            bool `json:"sync"`
}

// Decode decodes a serialized configuration
func Decode(path string) (*Config, error) {
	var cfg Config

	data, err := ioutil.ReadFile(filepath.Join(path, "config.json"))
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Encode encodes the configuration for storage
func (c *Config) Encode() ([]byte, error) {
	return json.Marshal(c)
}
