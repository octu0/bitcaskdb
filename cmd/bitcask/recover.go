package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"git.mills.io/prologic/bitcask"
	"git.mills.io/prologic/bitcask/internal"
	"git.mills.io/prologic/bitcask/internal/config"
	"git.mills.io/prologic/bitcask/internal/data/codec"
	"git.mills.io/prologic/bitcask/internal/index"
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
	maxValueSize := bitcask.DefaultMaxValueSize
	if cfg, err := config.Load(filepath.Join(path, "config.json")); err == nil {
		maxKeySize = cfg.MaxKeySize
		maxValueSize = cfg.MaxValueSize
	}

	if err := recoverIndex(filepath.Join(path, "index"), maxKeySize, dryRun); err != nil {
		log.WithError(err).Info("recovering index file")
		return 1
	}

	datafiles, err := internal.GetDatafiles(path)
	if err != nil {
		log.WithError(err).Info("coudn't list existing datafiles")
		return 1
	}
	for _, file := range datafiles {
		if err := recoverDatafile(file, maxKeySize, maxValueSize, dryRun); err != nil {
			log.WithError(err).Info("recovering data file")
			return 1
		}
	}

	return 0
}

func recoverIndex(path string, maxKeySize uint32, dryRun bool) error {
	t, found, err := index.NewIndexer().Load(path, maxKeySize)
	if err != nil && !index.IsIndexCorruption(err) {
		log.WithError(err).Info("opening the index file")
	}
	if !found {
		log.Info("index file doesn't exist, will be recreated on next run.")
		return nil
	}

	if err == nil {
		log.Debug("index file is not corrupted")
		return nil
	}
	log.Debugf("index file is corrupted: %v", err)

	if dryRun {
		log.Debug("dry-run mode, not writing to a file")
		return nil
	}

	// Leverage that t has the partiatially read tree even on corrupted files
	err = index.NewIndexer().Save(t, "index.recovered")
	if err != nil {
		return fmt.Errorf("writing the recovered index file: %w", err)
	}
	log.Debug("the index was recovered in the index.recovered new file")

	return nil
}

func recoverDatafile(path string, maxKeySize uint32, maxValueSize uint64, dryRun bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening the datafile: %w", err)
	}
	defer f.Close()
	_, file := filepath.Split(path)
	fr, err := os.OpenFile(fmt.Sprintf("%s.recovered", file), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("creating the recovered datafile: %w", err)
	}
	defer fr.Close()

	dec := codec.NewDecoder(f, maxKeySize, maxValueSize)
	enc := codec.NewEncoder(fr)
	e := internal.Entry{}
	for {
		_, err = dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if codec.IsCorruptedData(err) {
			log.Debugf("%s is corrupted, a best-effort recovery was done", file)
			return nil
		}
		if err != nil {
			return fmt.Errorf("unexpected error while reading datafile: %w", err)
		}
		if dryRun {
			continue
		}
		if _, err := enc.Encode(e); err != nil {
			return fmt.Errorf("writing to recovered datafile: %w", err)
		}
	}
	if err := os.Remove(fr.Name()); err != nil {
		return fmt.Errorf("can't remove temporal recovered datafile: %w", err)
	}
	log.Debugf("%s is not corrupted", file)
	return nil
}
