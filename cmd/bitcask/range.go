package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"git.mills.io/prologic/bitcask"
)

var rangeCmd = &cobra.Command{
	Use:     "range <start> <end>",
	Aliases: []string{},
	Short:   "Perform a range scan for keys from a start to end key",
	Long: `This performa a range scan for keys  starting with the given start
key and ending with the end key. This uses a Trie to search for matching keys
within the range and returns all matched keys.`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		start := args[0]
		end := args[1]

		os.Exit(_range(path, start, end))
	},
}

func init() {
	RootCmd.AddCommand(rangeCmd)
}

func _range(path, start, end string) int {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	err = db.Range([]byte(start), []byte(end), func(key []byte) error {
		value, err := db.Get(key)
		if err != nil {
			log.WithError(err).Error("error reading key")
			return err
		}

		fmt.Printf("%s\n", string(value))
		log.WithField("key", key).WithField("value", value).Debug("key/value")
		return nil
	})
	if err != nil {
		log.WithError(err).Error("error ranging over keys")
		return 1
	}

	return 0
}
