package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/prologic/bitcask"
)

var errNotAllDataWritten = errors.New("error: not all data written")

var exportCmd = &cobra.Command{
	Use:     "export",
	Aliases: []string{"backup", "dump"},
	Short:   "Export a database",
	Long: `This command allows you to export or dump/backup a database's
key/values into a long-term portable archival format suitable for backup and
restore purposes or migrating from older on-disk formats of Bitcask.

All key/value pairs are base64 encoded and serialized as JSON one pair per
line to form an output stream to either standard output or a file. You can
optionally compress the output with standard compression tools such as gzip.`,
	Args: cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		var output string

		path := viper.GetString("path")

		if len(args) == 1 {
			output = args[0]
		} else {
			output = "-"
		}

		os.Exit(export(path, output))
	},
}

func init() {
	RootCmd.AddCommand(exportCmd)

	exportCmd.PersistentFlags().IntP(
		"with-max-datafile-size", "", bitcask.DefaultMaxDatafileSize,
		"Maximum size of each datafile",
	)
	exportCmd.PersistentFlags().Uint32P(
		"with-max-key-size", "", bitcask.DefaultMaxKeySize,
		"Maximum size of each key",
	)
	exportCmd.PersistentFlags().Uint64P(
		"with-max-value-size", "", bitcask.DefaultMaxValueSize,
		"Maximum size of each value",
	)
}

type kvPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func export(path, output string) int {
	var (
		err error
		w   io.WriteCloser
	)

	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	if output == "-" {
		w = os.Stdout
	} else {
		w, err = os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0755)
		if err != nil {
			log.WithError(err).
				WithField("output", output).
				Error("error opening output for writing")
			return 1
		}
	}

	err = db.Fold(func(key []byte) error {
		value, err := db.Get(key)
		if err != nil {
			log.WithError(err).
				WithField("key", key).
				Error("error reading key")
			return err
		}

		kv := kvPair{
			Key:   base64.StdEncoding.EncodeToString([]byte(key)),
			Value: base64.StdEncoding.EncodeToString(value),
		}

		data, err := json.Marshal(&kv)
		if err != nil {
			log.WithError(err).
				WithField("key", key).
				Error("error serialzing key")
			return err
		}

		if n, err := w.Write(data); err != nil || n != len(data) {
			if err == nil && n != len(data) {
				err = errNotAllDataWritten
			}
			log.WithError(err).
				WithField("key", key).
				WithField("n", n).
				Error("error writing key")
			return err
		}

		if _, err := w.Write([]byte("\n")); err != nil {
			log.WithError(err).Error("error writing newline")
			return err
		}

		return nil
	})
	if err != nil {
		log.WithError(err).
			WithField("path", path).
			WithField("output", output).
			Error("error exporting keys")
		return 2
	}

	return 0
}
