package util

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Copy(t *testing.T) {
	assert := assert.New(t)
	t.Run("CopyDir", func(t *testing.T) {
		tempsrc, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer os.RemoveAll(tempsrc)
		var f *os.File

		tempdir, err := ioutil.TempDir(tempsrc, "")
		assert.NoError(err)

		f, err = os.OpenFile(filepath.Join(tempsrc, "file1"), os.O_WRONLY|os.O_CREATE, 0755)
		assert.NoError(err)
		n, err := f.WriteString("test123")
		assert.Equal(7, n)
		assert.NoError(err)
		f.Close()

		f, err = os.OpenFile(filepath.Join(tempsrc, "file2"), os.O_WRONLY|os.O_CREATE, 0755)
		assert.NoError(err)
		n, err = f.WriteString("test1234")
		assert.Equal(8, n)
		assert.NoError(err)
		f.Close()

		f, err = os.OpenFile(filepath.Join(tempsrc, "file3"), os.O_WRONLY|os.O_CREATE, 0755)
		assert.NoError(err)
		f.Close()

		tempdst, err := ioutil.TempDir("", "backup")
		assert.NoError(err)
		defer os.RemoveAll(tempdst)
		err = CopyFiles(tempdst, tempsrc, []string{"file3"})
		assert.NoError(err)
		buf := make([]byte, 10)

		exists := Exists(filepath.Join(tempdst, filepath.Base(tempdir)))
		assert.Equal(true, exists)

		f, err = os.Open(filepath.Join(tempdst, "file1"))
		assert.NoError(err)
		n, err = f.Read(buf[:7])
		assert.NoError(err)
		assert.Equal(7, n)
		assert.Equal([]byte("test123"), buf[:7])
		_, err = f.Read(buf)
		assert.Equal(io.EOF, err)
		f.Close()

		f, err = os.Open(filepath.Join(tempdst, "file2"))
		assert.NoError(err)
		n, err = f.Read(buf[:8])
		assert.NoError(err)
		assert.Equal(8, n)
		assert.Equal([]byte("test1234"), buf[:8])
		_, err = f.Read(buf)
		assert.Equal(io.EOF, err)
		f.Close()

		exists = Exists(filepath.Join(tempdst, "file3"))
		assert.Equal(false, exists)
	})
}
