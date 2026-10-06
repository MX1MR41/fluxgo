// Package offset persists consumer-group offsets: for each
// (group, topic, partition) the broker stores the next offset the group
// should consume, so consumers can resume after restarts.
//
// Layout: <dataDir>/__consumer_offsets/<groupID>/<topic>_<partition>.offset
// Each .offset file holds the 8-byte big-endian offset. Commits are written
// to a temp file, fsynced, then atomically renamed.
package offset

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/MX1MR41/fluxgo/internal/validate"
)

// ErrOffsetNotFound is returned when no offset was committed for the
// (group, topic, partition) triple.
var ErrOffsetNotFound = errors.New("offset: no committed offset")

const (
	offsetFileExt = ".offset"
	tempFileExt   = ".tmp"
	offsetsDir    = "__consumer_offsets"
)

// Manager stores and fetches committed offsets on disk.
type Manager struct {
	baseDir string
	logger  *slog.Logger
	mu      sync.RWMutex
}

// NewManager creates the offsets directory inside dataDir.
func NewManager(dataDir string, logger *slog.Logger) (*Manager, error) {
	if logger == nil {
		logger = slog.Default()
	}
	base := filepath.Join(dataDir, offsetsDir)
	if err := os.MkdirAll(base, 0o755); err != nil {
		return nil, fmt.Errorf("offset: failed to create offsets directory %s: %w", base, err)
	}
	return &Manager{baseDir: base, logger: logger}, nil
}

func (m *Manager) pathFor(groupID, topic string, partition uint32) (string, error) {
	if err := validate.GroupID(groupID); err != nil {
		return "", err
	}
	if err := validate.Topic(topic); err != nil {
		return "", err
	}
	file := fmt.Sprintf("%s_%d%s", topic, partition, offsetFileExt)
	return filepath.Join(m.baseDir, groupID, file), nil
}

// Commit durably records offset for the (group, topic, partition) triple.
func (m *Manager) Commit(groupID, topic string, partition uint32, offset uint64) error {
	path, err := m.pathFor(groupID, topic, partition)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("offset: failed to create group directory: %w", err)
	}

	tmp := path + tempFileExt
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return fmt.Errorf("offset: failed to open temp file: %w", err)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("offset: failed to write temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("offset: failed to sync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("offset: failed to close temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("offset: failed to rename temp file: %w", err)
	}
	// Best effort: fsync the directory so the rename itself survives a crash.
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// Fetch returns the committed offset for the (group, topic, partition)
// triple, or ErrOffsetNotFound.
func (m *Manager) Fetch(groupID, topic string, partition uint32) (uint64, error) {
	path, err := m.pathFor(groupID, topic, partition)
	if err != nil {
		return 0, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrOffsetNotFound
		}
		return 0, fmt.Errorf("offset: failed to open offset file: %w", err)
	}
	defer f.Close()

	var buf [8]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return 0, fmt.Errorf("offset: offset file %s is corrupt: %w", path, err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}
