package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"

	"git.mills.io/prologic/bitcask/internal"
)

var (
	bind    string
	debug   bool
	version bool
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <dbpath>\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.BoolVarP(&version, "version", "v", false, "display version information")
	flag.BoolVarP(&debug, "debug", "d", false, "enable debug logging")

	flag.StringVarP(&bind, "bind", "b", ":6379", "interface and port to bind to")
}

func main() {
	flag.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if version {
		fmt.Printf("bitcaskd version %s", internal.FullVersion())
		os.Exit(0)
	}

	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	path := flag.Arg(0)

	server, err := newServer(bind, path)
	if err != nil {
		log.WithError(err).Error("error creating server")
		os.Exit(2)
	}

	if err = server.Run(); err != nil {
		log.Fatal(err)
	}
}
