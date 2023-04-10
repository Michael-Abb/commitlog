package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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

func testAppendRead(t *testing.T, log *Log) {}

func testOutOfRangeErr(t *testing.T, log *Log) {}

func testInitExisting(t *testing.T, log *Log) {}

func testReader(t *testing.T, log *Log) {}

func testTruncate(t *testing.T, log *Log) {}
