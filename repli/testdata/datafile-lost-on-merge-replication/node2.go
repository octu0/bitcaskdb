package main

import (
  "log"
  "os"
  "path/filepath"
  "io/fs"
  "io"

  "github.com/octu0/bitcaskdb"
)

func main() {
  basedir := "/tmp/node2"
  db, err := bitcaskdb.Open(basedir, bitcaskdb.WithRepliClient("127.0.0.1", 4201), bitcaskdb.WithMaxDatafileSize(10 * 1024))
  if err != nil {
    log.Fatalf("%+v", err)
  }
  defer db.Close()

  if err := prompt("wait-init"); err != nil {
    log.Fatalf("%+v", err)
  }

  if err := prompt("wait-put"); err != nil {
    log.Fatalf("%+v", err)
  }
  lsdir(basedir)

  if err := prompt("wait-merge"); err != nil {
    log.Fatalf("%+v", err)
  }
  lsdir(basedir)

  e1, err := db.Get([]byte("hello1"))
  if err != nil {
    log.Fatalf("%+v", err)
  }
  defer e1.Close()

  data1, err := io.ReadAll(e1)
  if err != nil {
    log.Fatalf("%+v", err)
  }
  log.Printf("hello1 = %s", data1)

  // happen 'nil pointer dereference' by github.com/octu0/bitcaskdb.(*Bitcask).getLocked
  e2, err := db.Get([]byte("hello2"))
  if err != nil {
    log.Fatalf("%+v", err)
  }
  defer e2.Close()

  data2, err := io.ReadAll(e2)
  if err != nil {
    log.Fatalf("%+v", err)
  }
  log.Printf("hello2 = %s", data2)

  if err := prompt("done"); err != nil {
    log.Fatalf("%+v", err)
  }
}

func prompt(s string) error {
  if _, err := os.Stdout.Write([]byte(s)); err != nil {
    return err
  }
  buf := make([]byte, 1)
  if _, err := os.Stdin.Read(buf); err != nil {
    return err
  }
  return nil
}

func lsdir(basedir string) {
  filepath.Walk(basedir, func(path string, info fs.FileInfo, err error) error {
    if err != nil {
      return err
    }
    if info.IsDir() {
      return nil
    }
    println("F", info.Name(), info.Size())
    return nil
  })
}
