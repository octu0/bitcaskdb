package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/prologic/bitcask"
)

var scanCmd = &cobra.Command{
	Use:     "scan <prefix>",
	Aliases: []string{"search", "find"},
	Short:   "Perform a prefis scan for keys",
	Long: `This performa a prefix scan for keys  starting with the given
prefix. This uses a Trie to search for matching keys and returns all matched
keys.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		prefix := args[0]

		os.Exit(scan(path, prefix))
	},
}

func init() {
	RootCmd.AddCommand(scanCmd)
}

func scan(path, prefix string) int {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	err = db.Scan([]byte(prefix), func(key []byte) error {
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
		log.WithError(err).Error("error scanning keys")
		return 1
	}

	return 0
}
