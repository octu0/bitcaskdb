package main

import (
	"os"

	"github.com/prologic/bitcask"
	"github.com/prologic/bitcask/internal/config"
	"github.com/prologic/bitcask/internal/index"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var recoveryCmd = &cobra.Command{
	Use:     "recover",
	Aliases: []string{"recovery"},
	Short:   "Analyzes and recovers the index file for corruption scenarios",
	Long: `This analyze files to detect different forms of persistence corruption in 
persisted files. It also allows to recover the files to the latest point of integrity.
Recovered files have the .recovered extension`,
	Args: cobra.ExactArgs(0),
	PreRun: func(cmd *cobra.Command, args []string) {
		viper.BindPFlag("dry-run", cmd.Flags().Lookup("dry-run"))
	},
	Run: func(cmd *cobra.Command, args []string) {
		path := viper.GetString("path")
		dryRun := viper.GetBool("dry-run")
		os.Exit(recover(path, dryRun))
	},
}

func init() {
	RootCmd.AddCommand(recoveryCmd)
	recoveryCmd.Flags().BoolP("dry-run", "n", false, "Will only check files health without applying recovery if unhealthy")
}

func recover(path string, dryRun bool) int {
	maxKeySize := bitcask.DefaultMaxKeySize
	if cfg, err := config.Decode(path); err == nil {
		maxKeySize = cfg.MaxKeySize
	}

	t, found, err := index.ReadFromFile(path, maxKeySize)
	if err != nil && !index.IsIndexCorruption(err) {
		log.WithError(err).Info("error while opening the index file")
	}
	if !found {
		log.Info("index file doesn't exist, will be recreated on next run.")
		return 0
	}

	if err == nil {
		log.Debug("index file is not corrupted")
		return 0
	}
	log.Debugf("index file is corrupted: %v", err)

	if dryRun {
		log.Debug("dry-run mode, not writing to a file")
		return 0
	}

	fi, err := os.OpenFile("index.recovered", os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.WithError(err).Info("error while creating recovered index file")
		return 1
	}

	// Leverage that t has the partiatially read tree even on corrupted files
	err = index.WriteIndex(t, fi)
	if err != nil {
		log.WithError(err).Info("error while writing the recovered index file")

		fi.Close()
		return 1
	}
	err = fi.Close()
	if err != nil {
		log.WithError(err).Info("the recovered file index coudn't be saved correctly")
	}
	log.Debug("the index was recovered in the index.recovered new file")

	return 0
}
