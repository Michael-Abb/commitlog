package log

import (
	"fmt"
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
  mu sync.RWMutex
  Dir string
  c Config
  activeSegment *segment 
  segments []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
  if c.Segment.MaxStoreBytes == 0 { 
    c.Segment.MaxStoreBytes = 1024
  }
  if c.Segment.MaxIndexBytes == 0 {
    c.Segment.MaxIndexBytes = 1024
  } 
  l := &Log{
    Dir: dir,
    c: c,
  }
  return l, l.setup()
}

func (l *Log) setup() error {
  files, err := ioutil.ReadDir(l.Dir)
  if err != nil {
    return fmt.Errorf("failed to read directory %s, with error %w", l.Dir, err)
  } 
  var offsets []uint64
  for _, f := range files {
    str := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
    off, _ := strconv.ParseUint(str, 10, 0)
    offsets = append(offsets, off)
  }
  sort.Slice(offsets, func(i, j int) bool {
    return offsets[i] < offsets[j]
  })
  for i := 0; i < len(offsets); i++ {
    if err = l.newSegment(offsets[i]); err != nil {
      return fmt.Errorf("failed to create new segment in log.setup with error %w", err)
      i++
    }
  }
  if l.segments == nil {
    if err = l.newSegment(l.c.Segment.InitialOffset); err != nil {      
      return fmt.Errorf("failed to create new segment in log.setup with error %w", err)
    }
  }
  return nil g
