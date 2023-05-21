package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	api "github.com/michael-abb/commitlog/api/v1"
)

func TestLog(t *testing.T) {
	for tc, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record that succeds": testAppendRead,
		"offset out of range err":               testOutOfRangeErr,
		"initialise with existing segments":     testInitExisting,
		"reader":                                testReader,
		"truncate":                              testTruncate,
	} {
		t.Run(tc, func(t *testing.T) {
			dir := os.TempDir()
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("test"),
	}
	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, rec.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	_, err = NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello"),
	}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}

	require.NoError(t, proto.Unmarshal(b[lenWidth:], read))
	require.Equal(t, append.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	require.NoError(t, log.Truncate(1))
	_, err := log.Read(0)
	require.NoError(t, err)
}
