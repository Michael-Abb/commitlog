package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
  enc = binary.BigEndian
)

const (
  lenWidth = 8
)

type store struct {
  *os.File
  mu sync.Mutex
  buf *bufio.Writer
  size uint64
}

func (s *store) append(p []byte) (n uint64, pos uint64, err error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  
  pos = s.size
  
  if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
    return 0, 0, fmt.Errorf("failed to binary write in store.append with error %w", err)
  }

  w, err := s.buf.Write(p)

  if err != nil {
    return 0, 0, fmt.Errorf("failed to write buffer in store.append with error %w", err)
  }

  w += lenWidth

  s.size += uint64(w)
  return uint64(w), pos, nil
}


func newStore(f *os.File) (*store, error) {
  fi, err := os.Stat(f.Name())
  if err != nil {
    return nil, fmt.Errorf("failed to get stats for %s in newStore with error %w", f.Name(), err)
  }
  size := uint64(fi.Size())
  return &store{
    File: f,
    size: size,
    buf: bufio.NewWriter(f),
  }, nil
}