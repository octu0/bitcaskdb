package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/prologic/bitcask"
)

var mergeCmd = &cobra.Command{
	Use:     "merge",
	Aliases: []string{"clean", "compact", "defrag"},
	Short:   "Merges the Datafiles in the Database",
	Long: `This merges all non-active Datafiles in the Database and
compacts the data stored on disk. Old values are removed as well as deleted
keys.`,
	Args: cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		os.Exit(merge(path))
	},
}

func init() {
	RootCmd.AddCommand(mergeCmd)
}

func merge(path string) int {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}

	if err = db.Merge(); err != nil {
		log.WithError(err).Error("error merging database")
		return 1
	}

	return 0
}
