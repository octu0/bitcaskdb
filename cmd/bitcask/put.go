package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"git.mills.io/prologic/bitcask"
)

var putCmd = &cobra.Command{
	Use:     "put <key> [<value>]",
	Aliases: []string{"add", "set", "store"},
	Short:   "Adds a new Key/Value pair",
	Long: `This adds a new key/value pair or modifies an existing one.

If the value is not specified as an argument it is read from standard input.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		key := args[0]

		var value io.Reader
		if len(args) > 1 {
			value = bytes.NewBufferString(args[1])
		} else {
			value = os.Stdin
		}

		os.Exit(put(path, key, value))
	},
}

func init() {
	RootCmd.AddCommand(putCmd)
}

func put(path, key string, value io.Reader) int {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	data, err := ioutil.ReadAll(value)
	if err != nil {
		log.WithError(err).Error("error writing key")
		return 1
	}

	err = db.Put([]byte(key), data)
	if err != nil {
		log.WithError(err).Error("error writing key")
		return 1
	}

	return 0
}
