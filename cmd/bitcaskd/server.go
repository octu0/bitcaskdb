package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"

	"git.mills.io/prologic/bitcask"
)

type server struct {
	bind string
	db   *bitcask.Bitcask
}

func newServer(bind, dbpath string) (*server, error) {
	db, err := bitcask.Open(dbpath)
	if err != nil {
		log.WithError(err).WithField("dbpath", dbpath).Error("error opening database")
		return nil, err
	}

	return &server{
		bind: bind,
		db:   db,
	}, nil
}

func (s *server) handleSet(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 3 && len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	var ttl *time.Duration

	key := cmd.Args[1]
	value := cmd.Args[2]

	if len(cmd.Args) == 4 {
		val, n := binary.Varint(cmd.Args[3])
		if n <= 0 {
			conn.WriteError("ERR error parsing ttl")
			return
		}
		d := time.Duration(val) * time.Millisecond
		ttl = &d
	}

	if ttl != nil {
		if err := s.db.PutWithTTL(key, value, *ttl); err != nil {
			conn.WriteString(fmt.Sprintf("ERR: %s", err))
		}
	} else {
		if err := s.db.Put(key, value); err != nil {
			conn.WriteString(fmt.Sprintf("ERR: %s", err))
		}
	}

	conn.WriteString("OK")
}

func (s *server) handleGet(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	value, err := s.db.Get(key)
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(value)
	}
}

func (s *server) handleKeys(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	pattern := string(cmd.Args[1])

	// Fast-track condition for improved speed
	if pattern == "*" {
		conn.WriteArray(s.db.Len())
		for key := range s.db.Keys() {
			conn.WriteBulk(key)
		}
		return
	}

	// Prefix handling
	if strings.Count(pattern, "*") == 1 && strings.HasSuffix(pattern, "*") {
		prefix := strings.ReplaceAll(pattern, "*", "")
		count := 0
		keys := make([][]byte, 0)
		s.db.Scan([]byte(prefix), func(key []byte) error {
			keys = append(keys, key)
			count++
			return nil
		})
		conn.WriteArray(count)
		for _, key := range keys {
			conn.WriteBulk(key)
		}
		return
	}

	// No results means empty array
	conn.WriteArray(0)
}

func (s *server) handleExists(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	if s.db.Has(key) {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
}

func (s *server) handleDel(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	if err := s.db.Delete(key); err != nil {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (s *server) Shutdown() (err error) {
	err = s.db.Close()
	return
}

func (s *server) Run() (err error) {
	redServer := redcon.NewServerNetwork("tcp", s.bind,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				s.handleSet(cmd, conn)
			case "get":
				s.handleGet(cmd, conn)
			case "keys":
				s.handleKeys(cmd, conn)
			case "exists":
				s.handleExists(cmd, conn)
			case "del":
				s.handleDel(cmd, conn)
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			}
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		},
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		s := <-signals
		log.Infof("Shutdown server on signal %s", s)
		redServer.Close()
	}()

	if err := redServer.ListenAndServe(); err == nil {
		return s.Shutdown()
	}
	return
}
