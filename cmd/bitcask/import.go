package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/prologic/bitcask"
)

var importCmd = &cobra.Command{
	Use:     "import",
	Aliases: []string{"restore", "read"},
	Short:   "Import a database",
	Long: `This command allows you to import or restore a database from a
previous export/dump using the export command either creating a new database
or adding additional key/value pairs to an existing one.`,
	Args: cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		var input string

		path := viper.GetString("path")

		if len(args) == 1 {
			input = args[0]
		} else {
			input = "-"
		}

		os.Exit(_import(path, input))
	},
}

func init() {
	RootCmd.AddCommand(importCmd)
}

func _import(path, input string) int {
	var (
		err error
		r   io.ReadCloser
	)

	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	if input == "-" {
		r = os.Stdin
	} else {
		r, err = os.Open(input)
		if err != nil {
			log.WithError(err).
				WithField("input", input).
				Error("error opening input for reading")
			return 1
		}
	}

	var kv kvPair

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
			log.WithError(err).
				WithField("input", input).
				Error("error reading input")
			return 2
		}

		key, err := base64.StdEncoding.DecodeString(kv.Key)
		if err != nil {
			log.WithError(err).Error("error decoding key")
			return 2
		}

		value, err := base64.StdEncoding.DecodeString(kv.Value)
		if err != nil {
			log.WithError(err).Error("error decoding value")
			return 2
		}

		if err := db.Put(key, value); err != nil {
			log.WithError(err).Error("error writing key/value")
			return 2
		}
	}
	if err := scanner.Err(); err != nil {
		log.WithError(err).
			WithField("input", input).
			Error("error reading input")
		return 2

	}

	return 0
}
