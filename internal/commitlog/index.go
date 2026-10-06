package commitlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// indexEntryWidth is the on-disk size of one index entry:
// [8B relative offset][8B position in the .log file].
const indexEntryWidth = 16

// index maps relative record offsets to byte positions in the segment's log
// file. There is exactly one entry per record, so the entry for relative
// offset N always lives at byte N*indexEntryWidth. Writes use WriteAt at the
// tracked size, which keeps appends correct across process restarts without
// relying on the file's internal offset.
type index struct {
	mu   sync.RWMutex
	file *os.File
	size int64
}

func newIndex(f *os.File) (*index, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("commitlog: failed to stat index file: %w", err)
	}
	return &index{file: f, size: fi.Size()}, nil
}

// entries returns the number of whole index entries currently on disk.
func (idx *index) entries() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.size / indexEntryWidth
}

func (idx *index) ReadPositionForOffset(relativeOffset uint64) (uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.file == nil {
		return 0, ErrLogClosed
	}
	entryPos := int64(relativeOffset) * indexEntryWidth
	if entryPos < 0 || entryPos+indexEntryWidth > idx.size {
		return 0, fmt.Errorf("%w: relative offset %d", ErrIndexNotFound, relativeOffset)
	}
	entryBytes := make([]byte, indexEntryWidth)
	if _, err := idx.file.ReadAt(entryBytes, entryPos); err != nil {
		return 0, fmt.Errorf("commitlog: failed to read index entry %d: %w", relativeOffset, err)
	}
	return binary.BigEndian.Uint64(entryBytes[8:]), nil
}

// WriteEntry appends the entry for relativeOffset. It is a logic error to
// write entries out of order, and doing so is reported rather than silently
// corrupting the index.
func (idx *index) WriteEntry(relativeOffset uint64, position uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return ErrLogClosed
	}
	expected := uint64(idx.size / indexEntryWidth)
	if relativeOffset != expected {
		return fmt.Errorf("commitlog: index entry out of order: got relative offset %d, expected %d",
			relativeOffset, expected)
	}
	entry := make([]byte, indexEntryWidth)
	binary.BigEndian.PutUint64(entry[0:8], relativeOffset)
	binary.BigEndian.PutUint64(entry[8:16], position)
	if _, err := idx.file.WriteAt(entry, idx.size); err != nil {
		return fmt.Errorf("commitlog: failed to write index entry %d: %w", relativeOffset, err)
	}
	idx.size += indexEntryWidth
	return nil
}

// rewrite replaces the whole index with the given positions (used by crash
// recovery when the index and the log disagree).
func (idx *index) rewrite(positions []uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return ErrLogClosed
	}
	if err := idx.file.Truncate(0); err != nil {
		return fmt.Errorf("commitlog: failed to truncate index for rebuild: %w", err)
	}
	entry := make([]byte, indexEntryWidth)
	for i, pos := range positions {
		binary.BigEndian.PutUint64(entry[0:8], uint64(i))
		binary.BigEndian.PutUint64(entry[8:16], pos)
		if _, err := idx.file.WriteAt(entry, int64(i)*indexEntryWidth); err != nil {
			return fmt.Errorf("commitlog: failed to rewrite index entry %d: %w", i, err)
		}
	}
	idx.size = int64(len(positions)) * indexEntryWidth
	return idx.file.Sync()
}

// truncate drops the index down to whole entries (used to strip a torn
// trailing entry) and returns the number of entries kept.
func (idx *index) truncateToWholeEntries() (int64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return 0, ErrLogClosed
	}
	whole := idx.size / indexEntryWidth * indexEntryWidth
	if whole == idx.size {
		return idx.size / indexEntryWidth, nil
	}
	if err := idx.file.Truncate(whole); err != nil {
		return 0, fmt.Errorf("commitlog: failed to truncate torn index entry: %w", err)
	}
	idx.size = whole
	return whole / indexEntryWidth, nil
}

func (idx *index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.file == nil {
		return ErrLogClosed
	}
	return idx.file.Sync()
}

func (idx *index) Name() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.file == nil {
		return ""
	}
	return idx.file.Name()
}

func (idx *index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.file == nil {
		return nil
	}
	err := idx.file.Close()
	idx.file = nil
	idx.size = 0
	return err
}
