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

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  
  pos = s.size
  
  if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
    return 0, 0, fmt.Errorf("failed to binary write in store.append with error %w", err)
  }

  w, err := s.buf.Write(p)

  if err != nil {
    return 0, 0, fmt.Errorf("failed to write buffer in store.Append with error %w", err)
  }

  w += lenWidth

  s.size += uint64(w)
  return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
  s.mu.Lock()
  defer s.mu.Unlock()

  if err := s.buf.Flush(); err != nil {
    return nil, fmt.Errorf("failed to flush buffer in store.Read with error %w", err)
  }
  size := make([]byte, lenWidth)
  if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
    return nil, fmt.Errorf("failed to read file at position %d in store.Read with error %w", int64(pos), err)
  }
  b := make([]byte, enc.Uint64(size))
  if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
    return nil, fmt.Errorf("failed to read file at position %d in store.Read with error %w", int64(pos+lenWidth), err)
  }
  return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
  s.mu.Lock()
  defer s.mu.Unlock()
  if err := s.buf.Flush(); err != nil {
    return 0, fmt.Errorf("failed to flush write buffer in store.ReadAt with error %w", err)
  }
  return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
  s.mu.Lock()
  defer s.mu.Unlock()
  err := s.buf.Flush()
  if err != nil {
    return fmt.Errorf("failed to flush write buffer in store.Close with error %w", err)
  }
  return s.File.Close()
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
