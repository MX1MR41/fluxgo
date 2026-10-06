// Package commitlog implements FluxGo's storage engine: a persistent,
// append-only, segmented log for a single topic-partition.
//
// On-disk layout per segment (files named by zero-padded base offset):
//
//	00000000000000000000.log    [8B length][record][8B length][record]...
//	00000000000000000000.index  [8B relative offset][8B log position]...
//
// The index holds one entry per record, so lookups are O(1) inside a segment
// and O(log segments) across the log.
package commitlog

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrOffsetNotFound is returned when the offset is not present in the log.
	ErrOffsetNotFound = errors.New("commitlog: offset not found")
	// ErrOffsetOutOfRange is returned when the offset is older than the
	// earliest record still retained.
	ErrOffsetOutOfRange = errors.New("commitlog: offset out of range")
	// ErrReadPastEnd is returned when reading at or beyond the next offset
	// to be assigned (i.e. there is no new data yet).
	ErrReadPastEnd = errors.New("commitlog: read past end of log")
	// ErrLogClosed is returned on operations on a closed log.
	ErrLogClosed = errors.New("commitlog: log is closed")
	// ErrIndexNotFound is returned when an index entry is missing.
	ErrIndexNotFound = errors.New("commitlog: index entry not found")
	// ErrCorruptSegment is returned when a record on disk is inconsistent.
	ErrCorruptSegment = errors.New("commitlog: corrupt segment data")
)

// Record is a single message payload.
type Record []byte

// Config controls a single log's behavior.
type Config struct {
	// MaxSegmentBytes triggers a rollover to a new segment when the active
	// segment reaches this size. A single record may exceed it.
	MaxSegmentBytes int64
	// MaxLogBytes bounds the total size of the log; oldest segments are
	// deleted after appends while the total exceeds it. 0 disables.
	MaxLogBytes int64
	// FileSync fsyncs after every append when true (durable, slower).
	FileSync bool
}

// DefaultConfig returns sane defaults for a small broker.
func DefaultConfig() Config {
	return Config{
		MaxSegmentBytes: 16 * 1024 * 1024,
		MaxLogBytes:     1024 * 1024 * 1024,
		FileSync:        true,
	}
}

// Log is the commit log for one topic-partition. It is safe for concurrent
// use: reads proceed in parallel while appends are serialized.
type Log struct {
	mu     sync.RWMutex
	dir    string
	name   string
	config Config
	logger *slog.Logger

	segments      []*Segment // sorted by base offset
	activeSegment *Segment
	totalSize     int64

	closed bool
}

// Open loads the log stored in dir (creating it if necessary) and returns it
// ready for appends and reads. Crash recovery runs on every segment found.
func Open(dir, name string, config Config, logger *slog.Logger) (*Log, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("commitlog: failed to create log directory %s: %w", dir, err)
	}

	l := &Log{dir: dir, name: name, config: config, logger: logger}

	if err := l.loadSegments(); err != nil {
		return nil, fmt.Errorf("commitlog: failed to load segments for %s: %w", name, err)
	}

	if len(l.segments) == 0 {
		seg, err := openSegment(l.dir, 0, l.config, true, l.logger)
		if err != nil {
			return nil, fmt.Errorf("commitlog: failed to create initial segment for %s: %w", name, err)
		}
		l.segments = []*Segment{seg}
	}
	l.activeSegment = l.segments[len(l.segments)-1]
	for _, s := range l.segments {
		l.totalSize += s.Size()
	}
	return l, nil
}

// loadSegments discovers segment files in the log directory. Pairing is
// driven by .log files; an orphan .index is ignored (the segment's recovery
// rebuilds it), and stray files are skipped with a warning.
func (l *Log) loadSegments() error {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("commitlog: failed to read log directory %s: %w", l.dir, err)
	}

	var baseOffsets []uint64
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), logSuffix) {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(file.Name(), logSuffix), 10, 64)
		if err != nil {
			l.logger.Warn("commitlog: ignoring file with invalid name",
				"dir", l.dir, "file", file.Name())
			continue
		}
		baseOffsets = append(baseOffsets, base)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	l.segments = make([]*Segment, 0, len(baseOffsets))
	for i, base := range baseOffsets {
		// A crash can only tear the segment that was active at the time, i.e.
		// the last one on disk; with file_sync disabled a torn tail can also
		// survive in sealed segments, so those are reconciled too.
		reconcile := !l.config.FileSync || i == len(baseOffsets)-1
		seg, err := openSegment(l.dir, base, l.config, reconcile, l.logger)
		if err != nil {
			for _, s := range l.segments {
				s.Close()
			}
			l.segments = nil
			return fmt.Errorf("commitlog: failed to open segment %d: %w", base, err)
		}
		l.segments = append(l.segments, seg)
	}
	return nil
}

// Append adds the record to the log and returns its absolute offset. Size
// based retention runs after a successful append.
func (l *Log) Append(record Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}

	if l.activeSegment.IsFull() {
		if err := l.rollSegmentLocked(); err != nil {
			return 0, fmt.Errorf("commitlog: failed to roll segment for %s: %w", l.name, err)
		}
	}

	offset, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	l.totalSize += int64(len(record)) + recordLengthWidth

	if err := l.applyRetentionLocked(); err != nil {
		// Retention failures are non-fatal for the append itself; the record
		// is durably stored. Log loudly and keep going.
		l.logger.Error("commitlog: retention failed", "log", l.name, "error", err)
	}
	return offset, nil
}

// rollSegmentLocked seals the active segment and starts a new one. Caller
// must hold the write lock.
func (l *Log) rollSegmentLocked() error {
	// Make sure the sealed segment is durable before moving on.
	if err := l.activeSegment.index.Sync(); err != nil {
		l.logger.Warn("commitlog: failed to sync index before rolling",
			"log", l.name, "error", err)
	}
	if err := l.activeSegment.store.Sync(); err != nil {
		l.logger.Warn("commitlog: failed to sync log before rolling",
			"log", l.name, "error", err)
	}

	nextBase := l.activeSegment.NextOffset()
	seg, err := openSegment(l.dir, nextBase, l.config, true, l.logger)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.activeSegment = seg
	return nil
}

// Read returns the record at the given absolute offset.
func (l *Log) Read(offset uint64) (Record, error) {
	records, err := l.ReadBatch(offset, 1, 0)
	if err != nil {
		return nil, err
	}
	return records[0], nil
}

// ReadBatch returns up to maxRecords consecutive records starting at offset,
// crossing segment boundaries as needed. maxBytes bounds the total payload
// size returned (0 means no limit); a single record larger than maxBytes is
// still returned so consumers always make progress.
//
// Possible errors: ErrOffsetOutOfRange (offset is older than the retained
// data), ErrReadPastEnd (no new data at offset yet).
func (l *Log) ReadBatch(offset uint64, maxRecords int, maxBytes int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}
	if maxRecords < 1 {
		maxRecords = 1
	}

	// Read watermarks directly instead of via the locking accessors: holding
	// RLock here and acquiring it again inside LowestOffset/HighestOffset
	// would risk deadlock once a writer is queued (sync.RWMutex read locks
	// are not re-entrant).
	low := l.segments[0].BaseOffset()
	high := l.activeSegment.NextOffset()

	switch {
	case offset < low:
		return nil, ErrOffsetOutOfRange
	case offset >= high:
		return nil, ErrReadPastEnd
	}

	start := l.findSegmentIndexLocked(offset)
	records := make([]Record, 0, min(maxRecords, 64))
	total := 0

	for i := start; i < len(l.segments) && len(records) < maxRecords; i++ {
		seg := l.segments[i]
		segOffset := offset
		if i > start {
			segOffset = seg.BaseOffset()
		}
		remaining := 0
		if maxBytes > 0 {
			remaining = maxBytes - total
			if remaining <= 0 {
				break
			}
		}
		// The first record overall may exceed maxBytes (so consumers can
		// always make progress); once anything has been collected the limit
		// becomes strict, otherwise per-segment exceptions would compound.
		atLeastOne := len(records) == 0
		batch, err := seg.readBatch(segOffset, maxRecords-len(records), remaining, atLeastOne)
		if err != nil {
			// The first segment is guaranteed to contain offset; later
			// segments report ErrOffsetNotFound only when empty.
			if errors.Is(err, ErrOffsetNotFound) {
				continue
			}
			return nil, err
		}
		for _, rec := range batch {
			records = append(records, rec)
			total += len(rec) + recordLengthWidth
		}
	}

	if len(records) == 0 {
		return nil, ErrReadPastEnd
	}
	return records, nil
}

// findSegmentIndexLocked returns the index of the segment that contains (or
// should contain) offset. Caller must hold a lock.
func (l *Log) findSegmentIndexLocked(offset uint64) int {
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].BaseOffset() > offset
	})
	if i == 0 {
		return 0
	}
	return i - 1
}

// applyRetentionLocked deletes the oldest whole segments while the total log
// size exceeds MaxLogBytes. The active segment is never removed. Caller must
// hold the write lock.
func (l *Log) applyRetentionLocked() error {
	if l.config.MaxLogBytes <= 0 || l.totalSize <= l.config.MaxLogBytes || len(l.segments) <= 1 {
		return nil
	}

	var remove []*Segment
	var freed int64
	for _, seg := range l.segments[:len(l.segments)-1] {
		if l.totalSize-freed <= l.config.MaxLogBytes {
			break
		}
		remove = append(remove, seg)
		freed += seg.Size()
	}
	if len(remove) == 0 {
		return nil
	}

	l.segments = l.segments[len(remove):]
	l.totalSize -= freed
	l.logger.Info("commitlog: applying retention",
		"log", l.name, "segmentsRemoved", len(remove), "bytesFreed", freed)

	var errs []error
	for _, seg := range remove {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := seg.Remove(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close seals the log and all of its segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true

	var errs []error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, fmt.Errorf("segment %d: %w", seg.BaseOffset(), err))
		}
	}
	l.segments = nil
	l.activeSegment = nil
	l.totalSize = 0
	return errors.Join(errs...)
}

// Name returns the log's name ("topic_partition").
func (l *Log) Name() string { return l.name }

// Dir returns the log's data directory.
func (l *Log) Dir() string { return l.dir }

// HighestOffset returns the offset that will be assigned to the next append
// (i.e. one past the last stored record; 0 for an empty log).
func (l *Log) HighestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.activeSegment == nil {
		return 0
	}
	return l.activeSegment.NextOffset()
}

// LowestOffset returns the oldest offset still retained.
func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].BaseOffset()
}

// Size returns the total size of the log's data files in bytes.
func (l *Log) Size() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.totalSize
}

// SegmentCount returns the number of segments on disk.
func (l *Log) SegmentCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}
