package datafile

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	defaultIdFormat        string = "%016x-%016x"
	defaultIdFormatLen     int    = 33
	defaultDatafileSuffix  string = ".data"
	datafileFilenameFormat string = "%s.data"
	datafileGlobPattern    string = "*.data"
)

var (
	idGen = newIdGenerator(rand.NewSource(time.Now().UnixNano()))
)

type defaultIdGenerator struct {
	mutex    *sync.Mutex
	rnd      *rand.Rand
	lastTime int64
	lastRand int64
}

func (g *defaultIdGenerator) Next() (int64, int64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	return g.nextIdLocked()
}

func (g *defaultIdGenerator) nextIdLocked() (int64, int64) {
	now := g.now()
	r := g.rand(now)

	g.lastTime = now
	g.lastRand = r

	return now, r
}

func (g *defaultIdGenerator) now() int64 {
	return time.Now().UTC().UnixNano()
}

func (g *defaultIdGenerator) rand(now int64) int64 {
	if g.lastTime < now {
		return g.rnd.Int63()
	}
	return g.lastRand + 1
}

func newIdGenerator(src rand.Source) *defaultIdGenerator {
	return &defaultIdGenerator{
		mutex:    new(sync.Mutex),
		rnd:      rand.New(src),
		lastTime: 0,
		lastRand: 0,
	}
}

const (
	FileIDByteSize int = 8 + 8
)

type FileID struct {
	Time int64
	Rand int64
}

func (f FileID) Equal(target FileID) bool {
	return target.Time == f.Time && target.Rand == f.Rand
}

func (f FileID) Newer(target FileID) bool {
	if f.Equal(target) {
		return false
	}
	if f.Time < target.Time {
		return true
	}
	if f.Time == target.Time {
		if f.Rand < target.Rand {
			return true
		}
	}
	return false
}

func (f FileID) String() string {
	return fmt.Sprintf(defaultIdFormat, f.Time, f.Rand)
}

func CreateFileID(t, r int64) FileID {
	return FileID{
		Time: t,
		Rand: r,
	}
}

func NextFileID() FileID {
	return CreateFileID(idGen.Next())
}

func formatDatafilePath(path string, id FileID) string {
	return filepath.Join(path, fmt.Sprintf(datafileFilenameFormat, id.String()))
}

func IsDatafile(fileName string) bool {
	if strings.HasSuffix(fileName, ".data") != true {
		return false
	}
	index := strings.LastIndex(fileName, defaultDatafileSuffix)
	fileID := fileName[0:index]
	if len(fileID) != defaultIdFormatLen {
		return false
	}
	timeField := fileID[0:16]
	separator := fileID[16 : 16+1]
	randField := fileID[16+1:]
	if separator != "-" {
		return false
	}
	if _, err := strconv.ParseInt(timeField, 16, 64); err != nil {
		return false
	}
	if _, err := strconv.ParseInt(randField, 16, 64); err != nil {
		return false
	}
	return true
}

func GrepFileIds(fileNames []string) []FileID {
	grepFile := make([]string, 0, len(fileNames))
	for _, name := range fileNames {
		if IsDatafile(name) != true {
			continue
		}
		grepFile = append(grepFile, name)
	}
	sort.Slice(grepFile, func(i, j int) bool {
		return grepFile[i] < grepFile[j]
	})

	ids := make([]FileID, len(grepFile))
	for i, name := range grepFile {
		index := strings.LastIndex(name, defaultDatafileSuffix)
		fileID := name[0:index]
		timeField := fileID[0:16]
		randField := fileID[16+1:]
		timeValue, _ := strconv.ParseInt(timeField, 16, 64) // already check IsDatafile
		randValue, _ := strconv.ParseInt(randField, 16, 64) // already check IsDatafile

		ids[i] = CreateFileID(timeValue, randValue)
	}
	return ids
}

func GrepFileIdsFromDatafilePath(path string) ([]FileID, error) {
	fileNames, err := globDatafileNames(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return GrepFileIds(fileNames), nil
}

func globDatafileNames(path string) ([]string, error) {
	matchPaths, err := filepath.Glob(filepath.Join(path, datafileGlobPattern))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fileNames := make([]string, len(matchPaths))
	for i, match := range matchPaths {
		fileNames[i] = filepath.Base(match)
	}
	sort.Strings(fileNames)
	return fileNames, nil
}
