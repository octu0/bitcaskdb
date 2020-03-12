package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"

	"github.com/prologic/bitcask"
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
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	key := cmd.Args[1]
	value := cmd.Args[2]

	err := s.db.Lock()
	if err != nil {
		conn.WriteError("ERR " + fmt.Errorf("failed to lock db: %v", err).Error() + "")
		return
	}
	defer s.db.Unlock()

	if err := s.db.Put(key, value); err != nil {
		conn.WriteString(fmt.Sprintf("ERR: %s", err))
	} else {
		conn.WriteString("OK")
	}
}

func (s *server) handleGet(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	err := s.db.Lock()
	if err != nil {
		conn.WriteError("ERR " + fmt.Errorf("failed to lock db: %v", err).Error() + "")
		return
	}
	defer s.db.Unlock()

	value, err := s.db.Get(key)
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(value)
	}
}

func (s *server) handleKeys(cmd redcon.Command, conn redcon.Conn) {
	err := s.db.Lock()
	if err != nil {
		conn.WriteError("ERR " + fmt.Errorf("failed to lock db: %v", err).Error() + "")
		return
	}
	defer s.db.Unlock()

	conn.WriteArray(s.db.Len())
	for key := range s.db.Keys() {
		conn.WriteBulk(key)
	}
}

func (s *server) handleExists(cmd redcon.Command, conn redcon.Conn) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	key := cmd.Args[1]

	err := s.db.Lock()
	if err != nil {
		conn.WriteError("ERR " + fmt.Errorf("failed to lock db: %v", err).Error() + "")
		return
	}
	defer s.db.Unlock()

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

	err := s.db.Lock()
	if err != nil {
		conn.WriteError("ERR " + fmt.Errorf("failed to lock db: %v", err).Error() + "")
		return
	}
	defer s.db.Unlock()

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
