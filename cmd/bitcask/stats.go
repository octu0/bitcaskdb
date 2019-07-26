package main

import (
	"encoding/json"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/prologic/bitcask"
)

var statsCmd = &cobra.Command{
	Use:     "stats",
	Aliases: []string{},
	Short:   "Display statis about the Database",
	Long:    `This displays statistics about the Database"`,
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")

		os.Exit(stats(path))
	},
}

func init() {
	RootCmd.AddCommand(statsCmd)
}

func stats(path string) int {
	db, err := bitcask.Open(path)
	if err != nil {
		log.WithError(err).Error("error opening database")
		return 1
	}
	defer db.Close()

	stats, err := db.Stats()
	if err != nil {
		log.WithError(err).Error("error getting stats")
		return 1
	}

	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		log.WithError(err).Error("error marshalling stats")
		return 1
	}

	fmt.Println(string(data))

	return 0
}
