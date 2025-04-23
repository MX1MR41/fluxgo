package commitlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"

	recordLengthWidth = 8

	indexOffsetWidth = 8

	indexPositionWidth = 8
)

type Segment struct {
	mu sync.RWMutex

	dir        string
	baseOffset uint64
	nextOffset uint64
	maxBytes   int64

	store *os.File
	index *index

	storeSize int64

	fileSync bool
}

func newSegment(dir string, baseOffset uint64, config Config) (*Segment, error) {
	s := &Segment{
		dir:        dir,
		baseOffset: baseOffset,
		maxBytes:   config.MaxSegmentBytes,
		fileSync:   config.FileSync,
	}

	logPath := s.logPath()
	storeFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create log file %s: %w", logPath, err)
	}
	s.store = storeFile

	storeFi, err := storeFile.Stat()
	if err != nil {
		storeFile.Close()
		return nil, fmt.Errorf("failed to stat log file %s: %w", logPath, err)
	}
	s.storeSize = storeFi.Size()

	indexPath := s.indexPath()
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		storeFile.Close()
		return nil, fmt.Errorf("failed to open/create index file %s: %w", indexPath, err)
	}

	idx, err := newIndex(indexFile)
	if err != nil {
		storeFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to initialize index %s: %w", indexPath, err)
	}
	s.index = idx

	if idx.size > 0 {

		numEntries := uint64(idx.size / indexEntryWidth)
		s.nextOffset = baseOffset + numEntries
	} else {

		s.nextOffset = baseOffset
	}

	return s, nil
}

func (s *Segment) logPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, logSuffix))
}

func (s *Segment) indexPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, indexSuffix))
}

func (s *Segment) Append(record Record) (absoluteOffset uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store == nil || s.index == nil {
		return 0, ErrLogClosed
	}

	currentOffset := s.nextOffset
	relativeOffset := currentOffset - s.baseOffset
	currentPosition := s.storeSize

	recordLen := uint64(len(record))
	lenBuf := make([]byte, recordLengthWidth)
	binary.BigEndian.PutUint64(lenBuf, recordLen)

	n, err := s.store.Write(lenBuf)
	if err != nil {
		return 0, fmt.Errorf("failed to write record length to log %s: %w", s.store.Name(), err)
	}
	s.storeSize += int64(n)

	n, err = s.store.Write(record)
	if err != nil {

		return 0, fmt.Errorf("failed to write record data to log %s: %w", s.store.Name(), err)
	}
	s.storeSize += int64(n)

	err = s.index.WriteEntry(relativeOffset, uint64(currentPosition))
	if err != nil {

		return 0, fmt.Errorf("CRITICAL: failed to write index entry for offset %d (rel %d) at pos %d in %s: %w",
			currentOffset, relativeOffset, currentPosition, s.index.Name(), err)
	}

	if s.fileSync {

		if err := s.store.Sync(); err != nil {

			return 0, fmt.Errorf("failed to sync log file %s: %w", s.store.Name(), err)
		}

		if err := s.index.Sync(); err != nil {
			return 0, fmt.Errorf("failed to sync index file %s: %w", s.index.Name(), err)
		}
	}

	s.nextOffset++

	return currentOffset, nil
}

func (s *Segment) Read(absoluteOffset uint64) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.store == nil || s.index == nil {
		return nil, ErrLogClosed
	}

	if absoluteOffset < s.baseOffset || absoluteOffset >= s.nextOffset {
		return nil, ErrOffsetNotFound
	}

	relativeOffset := absoluteOffset - s.baseOffset

	position, err := s.index.ReadPositionForOffset(relativeOffset)
	if err != nil {

		return nil, fmt.Errorf("failed to find index entry for offset %d (rel %d) in %s: %w",
			absoluteOffset, relativeOffset, s.index.Name(), err)
	}

	lenBuf := make([]byte, recordLengthWidth)
	_, err = s.store.ReadAt(lenBuf, int64(position))
	if err != nil {
		if err == io.EOF {

			return nil, fmt.Errorf("read length failed (EOF) at pos %d for offset %d in %s: %w",
				position, absoluteOffset, s.store.Name(), ErrReadPastEnd)
		}
		return nil, fmt.Errorf("failed to read record length at pos %d for offset %d in %s: %w",
			position, absoluteOffset, s.store.Name(), err)
	}
	recordLen := binary.BigEndian.Uint64(lenBuf)

	recordData := make(Record, recordLen)

	dataPosition := int64(position) + int64(recordLengthWidth)
	_, err = s.store.ReadAt(recordData, dataPosition)
	if err != nil {
		if err == io.EOF {

			return nil, fmt.Errorf("read data failed (EOF) at pos %d for offset %d (len %d) in %s: %w",
				dataPosition, absoluteOffset, recordLen, s.store.Name(), ErrReadPastEnd)
		}
		return nil, fmt.Errorf("failed to read record data at pos %d for offset %d (len %d) in %s: %w",
			dataPosition, absoluteOffset, recordLen, s.store.Name(), err)
	}

	return recordData, nil
}

func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.maxBytes > 0 && s.storeSize >= s.maxBytes
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var storeErr, indexErr error

	if s.index != nil {
		indexErr = s.index.Close()
		s.index = nil
	}
	if s.store != nil {

		if !s.fileSync {
			if syncErr := s.store.Sync(); syncErr != nil {

				fmt.Fprintf(os.Stderr, "Warning: failed to sync log file %s on close: %v\n", s.store.Name(), syncErr)
			}
		}
		storeErr = s.store.Close()
		s.store = nil
	}

	if storeErr != nil || indexErr != nil {
		return fmt.Errorf("error closing segment %d: store_err=%w, index_err=%w", s.baseOffset, storeErr, indexErr)
	}
	return nil
}

func (s *Segment) BaseOffset() uint64 {

	return s.baseOffset
}

func (s *Segment) NextOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

func (s *Segment) Remove() error {

	logPath := s.logPath()
	indexPath := s.indexPath()

	var rmLogErr, rmIndexErr error

	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		rmLogErr = fmt.Errorf("failed to remove log file %s: %w", logPath, err)
	}

	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		rmIndexErr = fmt.Errorf("failed to remove index file %s: %w", indexPath, err)
	}

	if rmLogErr != nil || rmIndexErr != nil {
		return fmt.Errorf("error removing segment %d files: log_err=%w, index_err=%w", s.baseOffset, rmLogErr, rmIndexErr)
	}
	return nil
}

func (s *Segment) SanityCheck() error {

	return nil
}
