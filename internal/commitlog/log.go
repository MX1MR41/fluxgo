package commitlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrOffsetNotFound  = errors.New("offset not found")
	ErrLogClosed       = errors.New("log is closed")
	ErrSegmentNotFound = errors.New("segment for offset not found")
	ErrIndexNotFound   = errors.New("index entry for offset not found")
	ErrReadPastEnd     = errors.New("read past end of log")
	ErrInvalidOffset   = errors.New("invalid offset requested")
)

type Record []byte

type Config struct {
	Path            string
	MaxSegmentBytes int64
	MaxLogBytes     int64
	FileSync        bool
}

func DefaultConfig() Config {
	return Config{
		Path:            "./fluxgo-data/",
		MaxSegmentBytes: 1024 * 1024 * 16,
		MaxLogBytes:     1024 * 1024 * 1024,
		FileSync:        true,
	}
}

type Log interface {
	Append(record Record) (offset uint64, err error)
	Read(offset uint64) (Record, error)
	Close() error
	Name() string
	Dir() string
	HighestOffset() uint64
	LowestOffset() uint64

	io.Closer
}

type commitLog struct {
	mu       sync.RWMutex
	dir      string
	name     string
	config   Config
	segments []*Segment

	activeSegment *Segment
	totalSize     int64

	closed bool
}

func NewLog(dir string, name string, config Config) (Log, error) {

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", dir, err)
	}

	l := &commitLog{
		dir:    dir,
		name:   name,
		config: config,
	}

	if err := l.loadSegments(); err != nil {
		return nil, fmt.Errorf("failed to load segments for log %s: %w", name, err)
	}

	if len(l.segments) == 0 {
		initialSegment, err := newSegment(l.dir, 0, l.config)
		if err != nil {
			return nil, fmt.Errorf("failed to create initial segment for log %s: %w", name, err)
		}
		l.segments = append(l.segments, initialSegment)
		l.activeSegment = initialSegment
		l.totalSize = initialSegment.storeSize
	} else {

		l.activeSegment = l.segments[len(l.segments)-1]

		for _, s := range l.segments {
			l.totalSize += s.storeSize
		}
	}

	return l, nil
}

func (l *commitLog) loadSegments() error {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return fmt.Errorf("failed to read log directory %s: %w", l.dir, err)
	}

	var baseOffsets []uint64
	segmentFiles := make(map[uint64]bool)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		if strings.HasSuffix(fileName, logSuffix) || strings.HasSuffix(fileName, indexSuffix) {
			baseOffsetStr := strings.TrimSuffix(strings.TrimSuffix(fileName, logSuffix), indexSuffix)
			baseOffset, err := strconv.ParseUint(baseOffsetStr, 10, 64)
			if err != nil {

				fmt.Fprintf(os.Stderr, "Warning: Ignoring file with invalid name format in %s: %s\n", l.dir, fileName)
				continue
			}
			if _, found := segmentFiles[baseOffset]; !found {
				segmentFiles[baseOffset] = true
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	l.segments = make([]*Segment, 0, len(baseOffsets))

	for _, baseOffset := range baseOffsets {

		segment, err := newSegment(l.dir, baseOffset, l.config)
		if err != nil {

			l.cleanupSegments()
			return fmt.Errorf("failed to load segment with base offset %d: %w", baseOffset, err)
		}

		l.segments = append(l.segments, segment)
	}

	return nil
}

func (l *commitLog) cleanupSegments() {
	for _, s := range l.segments {
		s.Close()
	}
	l.segments = nil
}

func (l *commitLog) Append(record Record) (offset uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}

	if l.activeSegment.IsFull() {
		if err := l.rollSegment(); err != nil {
			return 0, fmt.Errorf("failed to roll segment for log %s: %w", l.name, err)
		}
	}

	offset, err = l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	l.totalSize += int64(len(record) + recordLengthWidth)

	return offset, nil
}

func (l *commitLog) rollSegment() error {

	if err := l.activeSegment.index.Sync(); err != nil {

		fmt.Fprintf(os.Stderr, "Warning: failed to sync index %s before rolling: %v\n", l.activeSegment.index.Name(), err)
	}
	if err := l.activeSegment.store.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to sync store %s before rolling: %v\n", l.activeSegment.store.Name(), err)
	}

	nextBaseOffset := l.activeSegment.NextOffset()
	newSegment, err := newSegment(l.dir, nextBaseOffset, l.config)
	if err != nil {
		return fmt.Errorf("failed to create new segment with base offset %d: %w", nextBaseOffset, err)
	}

	l.segments = append(l.segments, newSegment)
	l.activeSegment = newSegment

	l.totalSize += newSegment.storeSize

	return nil
}

func (l *commitLog) Read(offset uint64) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}

	segment := l.findSegment(offset)
	if segment == nil {

		highest := l.HighestOffset()
		if offset >= highest {
			return nil, ErrReadPastEnd
		}

		return nil, ErrOffsetNotFound
	}

	return segment.Read(offset)
}

func (l *commitLog) findSegment(offset uint64) *Segment {

	idx := sort.Search(len(l.segments), func(i int) bool {

		return l.segments[i].BaseOffset() > offset
	})

	if idx == 0 {
		return nil
	}
	targetSegment := l.segments[idx-1]

	if offset < targetSegment.NextOffset() {
		return targetSegment
	}

	return nil
}

func (l *commitLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true

	var closeErrors []error
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("failed to close segment %d: %w", segment.BaseOffset(), err))
		}
	}

	l.segments = nil
	l.activeSegment = nil
	l.totalSize = 0

	if len(closeErrors) > 0 {

		errorMessages := make([]string, len(closeErrors))
		for i, err := range closeErrors {
			errorMessages[i] = err.Error()
		}
		return errors.New(strings.Join(errorMessages, "; "))
	}

	return nil
}

func (l *commitLog) Name() string {

	return l.name
}

func (l *commitLog) Dir() string {
	return l.dir
}

func (l *commitLog) HighestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.activeSegment == nil {
		return 0
	}
	return l.activeSegment.NextOffset()
}

func (l *commitLog) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.segments) == 0 {
		return 0
	}

	return l.segments[0].BaseOffset()
}

func (l *commitLog) applyRetention() error {

	if l.config.MaxLogBytes <= 0 || l.totalSize <= l.config.MaxLogBytes || len(l.segments) <= 1 {
		return nil
	}

	fmt.Printf("Log %s: Applying retention. Current size: %d, Max size: %d\n", l.name, l.totalSize, l.config.MaxLogBytes)

	segmentsToRemove := make([]*Segment, 0)
	var sizeReduced int64

	for i := 0; i < len(l.segments)-1; i++ {
		segment := l.segments[i]
		if l.totalSize-sizeReduced <= l.config.MaxLogBytes {
			break
		}
		fmt.Printf("Log %s: Marking segment %d for removal (size %d)\n", l.name, segment.BaseOffset(), segment.storeSize)
		segmentsToRemove = append(segmentsToRemove, segment)
		sizeReduced += segment.storeSize
	}

	if len(segmentsToRemove) == 0 {
		return nil
	}

	l.segments = l.segments[len(segmentsToRemove):]
	l.totalSize -= sizeReduced

	var removalErrors []error
	for _, segment := range segmentsToRemove {
		fmt.Printf("Log %s: Closing and removing segment %d\n", l.name, segment.BaseOffset())

		if err := segment.Close(); err != nil {
			removalErrors = append(removalErrors, fmt.Errorf("failed to close segment %d for removal: %w", segment.BaseOffset(), err))

		}

		if err := segment.Remove(); err != nil {
			removalErrors = append(removalErrors, fmt.Errorf("failed to remove files for segment %d: %w", segment.BaseOffset(), err))
		}
	}

	if len(removalErrors) > 0 {
		errorMessages := make([]string, len(removalErrors))
		for i, err := range removalErrors {
			errorMessages[i] = err.Error()
		}

		return fmt.Errorf("errors occurred during log retention cleanup: %s", strings.Join(errorMessages, "; "))
	}

	fmt.Printf("Log %s: Retention applied. New size: %d, Segments left: %d\n", l.name, l.totalSize, len(l.segments))
	return nil
}
