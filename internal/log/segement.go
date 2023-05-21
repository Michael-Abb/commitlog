package log

import (
	"fmt"
	"os"
	"path"

	"google.golang.org/protobuf/proto"

	api "github.com/michael-abb/commitlog/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	c                      Config
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, fmt.Errorf("failed to read from index in segmenet.Read with error %w", err)
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read from store in segement.Read with error %w", err)
	}

	r := &api.Record{}

	err = proto.Unmarshal(p, r)

	return r, err
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.c.Segment.MaxStoreBytes ||
		s.index.size >= s.c.Segment.MaxIndexBytes
}

func (s *segment) Append(r *api.Record) (uint64, error) {
	cur := s.nextOffset
	r.Offset = cur
	p, err := proto.Marshal(r)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal, in segment.Append with error %w", err)
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, fmt.Errorf("failed to append to store, in segment.Append with error %w", err)
	}

	if err := s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, fmt.Errorf("failed to write to index, in segment.Append with error %w", err)
	}

	s.nextOffset++
	return cur, err
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		c:          c,
	}

	var err error

	sf, err := os.OpenFile(
		fmt.Sprintf("%d%s", baseOffset, ".store"),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open file, in segment.newSegment with error %w", err)
	}

	if s.store, err = newStore(sf); err != nil {
		return nil, fmt.Errorf(
			"failed to create new store, in segment.newSegment with error %w",
			err,
		)
	}

	idxf, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open file, in segment.newSegment with error %w", err)
	}

	if s.index, err = newIndex(idxf, c); err != nil {
		return nil, fmt.Errorf(
			"failed to create new index, in segment.newSegement with error %w",
			err,
		)
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
