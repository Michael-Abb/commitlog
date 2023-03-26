package server

import (
	"errors"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffSetNotFound = errors.New("offset not found")

func (l *Log) Append(r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.Offset = uint64(len(l.records))
	l.records = append(l.records, r)
	return r.Offset, nil
}

func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffSetNotFound
	}
	return l.records[offset], nil
}

func NewLog() *Log {
	return &Log{}
}
