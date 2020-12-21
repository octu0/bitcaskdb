package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config contains the bitcask configuration parameters
type Config struct {
	MaxDatafileSize         int    `json:"max_datafile_size"`
	MaxKeySize              uint32 `json:"max_key_size"`
	MaxValueSize            uint64 `json:"max_value_size"`
	Sync                    bool   `json:"sync"`
	AutoRecovery            bool   `json:"autorecovery"`
	DBVersion               uint32 `json:"db_version"`
	DirFileModeBeforeUmask  os.FileMode
	FileFileModeBeforeUmask os.FileMode
}

// Load loads a configuration from the given path
func Load(path string) (*Config, error) {
	var cfg Config

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Save saves the configuration to the provided path
func (c *Config) Save(path string) error {

	data, err := json.Marshal(c)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path, data, c.FileFileModeBeforeUmask)
	if err != nil {
		return err
	}

	return nil
}
