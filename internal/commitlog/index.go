package commitlog

import (
	"encoding/binary"
	"os"
	"sync"
)

const (
	indexEntryWidth = 16
)

type index struct {
	mu   sync.RWMutex
	file *os.File
	size int64
}

func newIndex(f *os.File) (*index, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	idx := &index{
		file: f,
		size: fi.Size(),
	}
	return idx, nil
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

func (idx *index) ReadPositionForOffset(relativeOffset uint64) (pos uint64, err error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.file == nil {
		return 0, ErrLogClosed
	}

	if idx.size == 0 {
		return 0, ErrIndexNotFound
	}

	entryPosition := int64(relativeOffset * indexEntryWidth)

	if entryPosition+indexEntryWidth > idx.size {

		return 0, ErrIndexNotFound
	}

	entryBytes := make([]byte, indexEntryWidth)
	_, err = idx.file.ReadAt(entryBytes, entryPosition)
	if err != nil {

		return 0, err
	}

	pos = binary.BigEndian.Uint64(entryBytes[8:])

	return pos, nil
}

func (idx *index) WriteEntry(relativeOffset uint64, position uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.file == nil {
		return ErrLogClosed
	}

	entry := make([]byte, indexEntryWidth)
	binary.BigEndian.PutUint64(entry[0:8], relativeOffset)
	binary.BigEndian.PutUint64(entry[8:16], position)

	_, err := idx.file.Write(entry)
	if err != nil {
		return err
	}

	idx.size += indexEntryWidth

	return nil
}

func (idx *index) Name() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.file == nil {
		return ""
	}
	return idx.file.Name()
}

func (idx *index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.file == nil {
		return ErrLogClosed
	}
	return idx.file.Sync()
}
