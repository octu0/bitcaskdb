package main

import (
  "log"
  "os"
  "fmt"
  "bytes"
  "path/filepath"
  "io/fs"

  "github.com/octu0/bitcaskdb"
)

func main() {
  basedir := "/tmp/node1"
  db, err := bitcaskdb.Open(basedir, bitcaskdb.WithRepli("127.0.0.1", 4201), bitcaskdb.WithMaxDatafileSize(10 * 1024))
  if err != nil {
    log.Fatalf("%+v", err)
  }
  defer db.Close()

  if err := prompt("init"); err != nil {
    log.Fatalf("%+v", err)
  }

  data := bytes.Repeat([]byte("123"), 100)
  for i := 0; i < 100; i += 1 {
    db.PutBytes([]byte(fmt.Sprintf("%d", i)), data)
  }

  if err := prompt("put"); err != nil {
    log.Fatalf("%+v", err)
  }
  lsdir(basedir)

  db.PutBytes([]byte("hello1"), []byte("world1"))

  if err := db.Merge(); err != nil {
    log.Fatalf("%+v", err)
  }
  if err := prompt("merge"); err != nil {
    log.Fatalf("%+v", err)
  }
  lsdir(basedir)

  db.PutBytes([]byte("hello2"), []byte("world2"))

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
