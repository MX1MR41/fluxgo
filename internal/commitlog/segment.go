package commitlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"

	// recordLengthWidth is the size of the per-record length prefix.
	recordLengthWidth = 8
)

// Segment is one pair of .log/.index files covering offsets
// [baseOffset, nextOffset). Records are appended to the .log file as
// [8B length][payload] and mirrored by one 16-byte index entry per record.
//
// The .log file is the source of truth: the index is an acceleration
// structure that recovery can always rebuild by scanning the log.
type Segment struct {
	mu sync.RWMutex

	dir        string
	baseOffset uint64
	nextOffset uint64
	maxBytes   int64

	store *os.File
	index *index

	storeSize int64
	fileSync  bool
}

// openSegment opens (or creates) the segment with the given base offset. When
// reconcile is true, the log file is scanned and brought into agreement with
// the index (crash recovery); when false, the index is trusted as-is, which
// is safe for sealed segments while fileSync is on.
func openSegment(dir string, baseOffset uint64, cfg Config, reconcile bool, logger *slog.Logger) (*Segment, error) {
	s := &Segment{
		dir:        dir,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		maxBytes:   cfg.MaxSegmentBytes,
		fileSync:   cfg.FileSync,
	}

	storeFile, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, fmt.Errorf("commitlog: failed to open log file: %w", err)
	}
	s.store = storeFile

	// The index deliberately does NOT use O_APPEND: entries are written with
	// WriteAt at the tracked size, which keeps appends correct across
	// restarts (V1 used Write on a file opened without O_APPEND, so after a
	// restart new entries overwrote the index from byte 0).
	indexFile, err := os.OpenFile(s.indexPath(), os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		s.store.Close()
		return nil, fmt.Errorf("commitlog: failed to open index file: %w", err)
	}
	idx, err := newIndex(indexFile)
	if err != nil {
		s.store.Close()
		indexFile.Close()
		return nil, err
	}
	s.index = idx

	storeFi, err := storeFile.Stat()
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("commitlog: failed to stat log file: %w", err)
	}
	s.storeSize = storeFi.Size()

	if err := s.recover(reconcile, logger); err != nil {
		s.Close()
		return nil, fmt.Errorf("commitlog: failed to recover segment %d: %w", baseOffset, err)
	}
	return s, nil
}

// recover brings the segment into a consistent state after a possible crash.
func (s *Segment) recover(reconcile bool, logger *slog.Logger) error {
	entries, err := s.index.truncateToWholeEntries()
	if err != nil {
		return err
	}

	if !reconcile {
		// Sealed segment in durable mode: trust the index. An index that is
		// empty while the log has data can only mean the index was lost, so
		// rebuild that case.
		if entries == 0 && s.storeSize > 0 {
			return s.rebuildIndex(logger)
		}
		s.nextOffset = s.baseOffset + uint64(entries)
		return nil
	}

	// Crash-recovery path: scan the log (the source of truth), truncate any
	// torn tail, and rebuild the index if it disagrees with the log.
	positions, canonicalSize, err := s.scanLog()
	if err != nil {
		return err
	}
	if canonicalSize < s.storeSize {
		logger.Warn("commitlog: truncating partial record tail",
			"segment", s.baseOffset, "from", s.storeSize, "to", canonicalSize)
		if err := s.store.Truncate(canonicalSize); err != nil {
			return fmt.Errorf("commitlog: failed to truncate log: %w", err)
		}
		if err := s.store.Sync(); err != nil {
			return fmt.Errorf("commitlog: failed to sync truncated log: %w", err)
		}
		s.storeSize = canonicalSize
	}

	needRebuild := int64(len(positions)) != entries
	if !needRebuild && entries > 0 {
		// Counts match; spot-check that the last entry agrees with the scan.
		last, err := s.index.ReadPositionForOffset(uint64(entries - 1))
		if err != nil || last != positions[entries-1] {
			needRebuild = true
		}
	}
	if needRebuild {
		logger.Warn("commitlog: index out of sync with log, rebuilding",
			"segment", s.baseOffset, "indexEntries", entries, "logRecords", len(positions))
		if err := s.index.rewrite(positions); err != nil {
			return err
		}
	}
	s.nextOffset = s.baseOffset + uint64(len(positions))
	return nil
}

// scanLog walks the log file and returns the start position of every complete
// record plus the offset just past the last complete one. A partial or
// corrupt tail is simply not included.
func (s *Segment) scanLog() (positions []uint64, canonicalSize int64, err error) {
	lenBuf := make([]byte, recordLengthWidth)
	pos := int64(0)
	for pos+recordLengthWidth <= s.storeSize {
		if _, err := s.store.ReadAt(lenBuf, pos); err != nil {
			return nil, 0, fmt.Errorf("commitlog: scan failed at position %d: %w", pos, err)
		}
		recLen := binary.BigEndian.Uint64(lenBuf)
		end := pos + recordLengthWidth + int64(recLen)
		if end > s.storeSize {
			break // torn or corrupt tail: keep everything before it
		}
		positions = append(positions, uint64(pos))
		pos = end
	}
	return positions, pos, nil
}

// rebuildIndex rewrites the index from a full log scan.
func (s *Segment) rebuildIndex(logger *slog.Logger) error {
	positions, canonicalSize, err := s.scanLog()
	if err != nil {
		return err
	}
	if canonicalSize < s.storeSize {
		if err := s.store.Truncate(canonicalSize); err != nil {
			return fmt.Errorf("commitlog: failed to truncate log: %w", err)
		}
		s.storeSize = canonicalSize
	}
	if err := s.index.rewrite(positions); err != nil {
		return err
	}
	s.nextOffset = s.baseOffset + uint64(len(positions))
	logger.Warn("commitlog: rebuilt index from log",
		"segment", s.baseOffset, "records", len(positions))
	return nil
}

// Append writes the record and its index entry and returns the assigned
// absolute offset.
func (s *Segment) Append(record Record) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store == nil || s.index == nil {
		return 0, ErrLogClosed
	}

	offset := s.nextOffset
	position := s.storeSize

	// Write length prefix and payload in a single write call.
	buf := make([]byte, recordLengthWidth+len(record))
	binary.BigEndian.PutUint64(buf, uint64(len(record)))
	copy(buf[recordLengthWidth:], record)
	n, err := s.store.Write(buf)
	s.storeSize += int64(n)
	if err != nil {
		return 0, fmt.Errorf("commitlog: failed to write record at offset %d: %w", offset, err)
	}

	if err := s.index.WriteEntry(offset-s.baseOffset, uint64(position)); err != nil {
		return 0, fmt.Errorf("commitlog: failed to index record at offset %d: %w", offset, err)
	}

	if s.fileSync {
		if err := s.store.Sync(); err != nil {
			return 0, fmt.Errorf("commitlog: failed to sync log file: %w", err)
		}
		if err := s.index.Sync(); err != nil {
			return 0, fmt.Errorf("commitlog: failed to sync index file: %w", err)
		}
	}

	s.nextOffset++
	return offset, nil
}

// Read returns the record at the given absolute offset.
func (s *Segment) Read(offset uint64) (Record, error) {
	records, err := s.readBatch(offset, 1, 0, true)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, ErrOffsetNotFound
	}
	return records[0], nil
}

// ReadBatch reads up to maxRecords records starting at the absolute offset,
// stopping when the segment ends or when the accumulated size would exceed
// maxBytes (a single record larger than maxBytes is still returned so
// consumers always make progress).
func (s *Segment) ReadBatch(offset uint64, maxRecords int, maxBytes int) ([]Record, error) {
	return s.readBatch(offset, maxRecords, maxBytes, true)
}

// readBatch implements the read paths above. When atLeastOne is false the
// maxBytes limit is strict (used when the caller aggregates across segments).
func (s *Segment) readBatch(offset uint64, maxRecords int, maxBytes int, atLeastOne bool) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.store == nil || s.index == nil {
		return nil, ErrLogClosed
	}
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, ErrOffsetNotFound
	}

	position, err := s.index.ReadPositionForOffset(offset - s.baseOffset)
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0, min(maxRecords, 64))
	total := 0
	lenBuf := make([]byte, recordLengthWidth)

	for len(records) < maxRecords {
		pos := int64(position)
		if pos+recordLengthWidth > s.storeSize {
			break // end of segment
		}
		if _, err := s.store.ReadAt(lenBuf, pos); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("commitlog: failed to read record length at %d: %w", pos, err)
		}
		recLen := binary.BigEndian.Uint64(lenBuf)
		end := pos + recordLengthWidth + int64(recLen)
		if end > s.storeSize {
			return nil, fmt.Errorf("%w: record at position %d (len %d) runs past log size %d",
				ErrCorruptSegment, pos, recLen, s.storeSize)
		}
		recSize := recordLengthWidth + int(recLen)
		if maxBytes > 0 && total+recSize > maxBytes && !(atLeastOne && len(records) == 0) {
			break
		}
		data := make(Record, recLen)
		if _, err := s.store.ReadAt(data, pos+recordLengthWidth); err != nil {
			return nil, fmt.Errorf("commitlog: failed to read record at %d: %w", pos, err)
		}
		records = append(records, data)
		total += recSize
		position = uint64(end)
	}
	return records, nil
}

func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxBytes > 0 && s.storeSize >= s.maxBytes
}

func (s *Segment) BaseOffset() uint64 { return s.baseOffset }

func (s *Segment) NextOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.storeSize
}

func (s *Segment) logPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, logSuffix))
}

func (s *Segment) indexPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, indexSuffix))
}

// Close flushes (best effort) and closes both files.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	if s.index != nil {
		if err := s.index.Close(); err != nil {
			errs = append(errs, err)
		}
		s.index = nil
	}
	if s.store != nil {
		if err := s.store.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync on close: %w", err))
		}
		if err := s.store.Close(); err != nil {
			errs = append(errs, err)
		}
		s.store = nil
	}
	return errors.Join(errs...)
}

// Remove deletes the segment's files. The segment must be closed first.
func (s *Segment) Remove() error {
	var errs []error
	if err := os.Remove(s.logPath()); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if err := os.Remove(s.indexPath()); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
