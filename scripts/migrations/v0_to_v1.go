package migrations

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"git.mills.io/prologic/bitcask/internal"
)

const (
	keySize                 = 4
	valueSize               = 8
	checksumSize            = 4
	ttlSize                 = 8
	defaultDatafileFilename = "%09d.data"
)

func ApplyV0ToV1(dir string, maxDatafileSize int) error {
	temp, err := prepare(dir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	err = apply(dir, temp, maxDatafileSize)
	if err != nil {
		return err
	}
	return cleanup(dir, temp)
}

func prepare(dir string) (string, error) {
	return ioutil.TempDir(dir, "migration")
}

func apply(dir, temp string, maxDatafileSize int) error {
	datafilesPath, err := internal.GetDatafiles(dir)
	if err != nil {
		return err
	}
	var id, newOffset int
	datafile, err := getNewDatafile(temp, id)
	if err != nil {
		return err
	}
	id++
	for _, p := range datafilesPath {
		df, err := os.Open(p)
		if err != nil {
			return err
		}
		var off int64
		for {
			entry, err := getSingleEntry(df, off)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if newOffset+len(entry) > maxDatafileSize {
				err = datafile.Sync()
				if err != nil {
					return err
				}
				datafile, err = getNewDatafile(temp, id)
				if err != nil {
					return err
				}
				id++
				newOffset = 0
			}
			newEntry := make([]byte, len(entry)+ttlSize)
			copy(newEntry[:len(entry)], entry)
			n, err := datafile.Write(newEntry)
			if err != nil {
				return err
			}
			newOffset += n
			off += int64(len(entry))
		}
	}
	return datafile.Sync()
}

func cleanup(dir, temp string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			err := os.RemoveAll(path.Join([]string{dir, file.Name()}...))
			if err != nil {
				return err
			}
		}
	}
	files, err = ioutil.ReadDir(temp)
	if err != nil {
		return err
	}
	for _, file := range files {
		err := os.Rename(
			path.Join([]string{temp, file.Name()}...),
			path.Join([]string{dir, file.Name()}...),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func getNewDatafile(path string, id int) (*os.File, error) {
	fn := filepath.Join(path, fmt.Sprintf(defaultDatafileFilename, id))
	return os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0640)
}

func getSingleEntry(f *os.File, offset int64) ([]byte, error) {
	prefixBuf, err := readPrefix(f, offset)
	if err != nil {
		return nil, err
	}
	actualKeySize, actualValueSize := getKeyValueSize(prefixBuf)
	entryBuf, err := read(f, uint64(actualKeySize)+actualValueSize+checksumSize, offset+keySize+valueSize)
	if err != nil {
		return nil, err
	}
	return append(prefixBuf, entryBuf...), nil
}

func readPrefix(f *os.File, offset int64) ([]byte, error) {
	prefixBuf := make([]byte, keySize+valueSize)
	_, err := f.ReadAt(prefixBuf, offset)
	if err != nil {
		return nil, err
	}
	return prefixBuf, nil
}

func read(f *os.File, bufSize uint64, offset int64) ([]byte, error) {
	buf := make([]byte, bufSize)
	_, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func getKeyValueSize(buf []byte) (uint32, uint64) {
	actualKeySize := binary.BigEndian.Uint32(buf[:keySize])
	actualValueSize := binary.BigEndian.Uint64(buf[keySize:])
	return actualKeySize, actualValueSize
}
