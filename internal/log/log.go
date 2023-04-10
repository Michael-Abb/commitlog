package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/michael-abb/commitlog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	c             Config
	activeSegment *segment
	segments      []*segment
}

type originReader struct {
	*store
	off int64
}

func (o originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()

	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = &originReader{s.store, 0}
	}
	return io.MultiReader(readers...)
}

// Truncate removes all segments where the highest offset is lower that the lowest offset
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segs []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segs = append(segs, s)
	}
	l.segments = segs
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.c)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
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
		c:   c,
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

	return nil
}

func (l *Log) Append(r *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(r)

	if err != nil {
		return 0, fmt.Errorf("failed to append record in log.Append, with error %w", err)
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var s *segment

	for _, segment := range l.segments {
		if s.baseOffset <= off && off < s.nextOffset {
			s = segment
			break
		}
	}

	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}
