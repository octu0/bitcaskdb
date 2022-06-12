package bitcaskdb

//import(
//  "path"
//  "testing"
//
//	"github.com/stretchr/testify/require"
//)

/*todo
func TestAutoRecovery(t *testing.T) {
	withAutoRecovery := []bool{false, true}

	for _, autoRecovery := range withAutoRecovery {
		t.Run(fmt.Sprintf("%v", autoRecovery), func(t *testing.T) {
			require := require.New(t)
			testdir, err := ioutil.TempDir("", "bitcask")
			require.NoError(err)
			db, err := Open(testdir)
			require.NoError(err)

			// Insert 10 key-value pairs and verify all is ok.
			makeKeyVal := func(i int) ([]byte, []byte) {
				return []byte(fmt.Sprintf("foo%d", i)), []byte(fmt.Sprintf("bar%d", i))
			}
			n := 10
			for i := 0; i < n; i++ {
				key, val := makeKeyVal(i)
				err = db.PutBytes(key, val)
				require.NoError(err)
			}
			for i := 0; i < n; i++ {
				key, val := makeKeyVal(i)
				rval, err := db.Get(key)
				require.NoError(err)
        defer rval.Close()
        data, err2 := io.ReadAll(rval)
				require.NoError(err2)
				require.Equal(val, data)
			}
			err = db.Close()
			require.NoError(err)

			// Corrupt the last inserted key
			f, err := os.OpenFile(path.Join(testdir, "000000000.data"), os.O_RDWR, 0755)
			require.NoError(err)
			fi, err := f.Stat()
			require.NoError(err)
			err = f.Truncate(fi.Size() - 1)
			require.NoError(err)
			err = f.Close()
			require.NoError(err)

			db, err = Open(testdir, WithAutoRecovery(autoRecovery))
			require.NoError(err)
			defer db.Close()
			// Check that all values but the last are still intact.
			for i := 0; i < 9; i++ {
				key, val := makeKeyVal(i)
				rval, err := db.Get(key)
				require.NoError(err)
        defer rval.Close()
        data, err := io.ReadAll(rval)
				require.NoError(err)

				require.Equal(val, data)
			}
			// Check the index has no more keys than non-corrupted ones.
			// i.e: all but the last one.
			numKeys := 0
      for _ = range db.Keys() {
				numKeys++
			}
			if autoRecovery != true {
				// We are opening without autorepair, and thus are
				// in a corrupted state. The index isn't coherent with
				// the datafile.
				require.Equal(n, numKeys)
				return
			}

			require.Equal(n-1, numKeys, "The index should have n-1 keys")

			// Double-check explicitly the corrupted one isn't here.
			// This check is redundant considering the last two checks,
			// but doesn't hurt.
			corrKey, _ := makeKeyVal(9)
			_, err = db.Get(corrKey)
			require.Equal(ErrKeyNotFound, err)
		})
	}
}
*/
