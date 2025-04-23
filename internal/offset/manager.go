package offset

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrOffsetNotFound = errors.New("committed offset not found")
)

const (
	offsetFileExtension = ".offset"
	tempFileSuffix      = ".tmp"
	offsetsDirName      = "__consumer_offsets"
)

type Manager struct {
	mu      sync.RWMutex
	baseDir string
}

func NewManager(dataDir string) (*Manager, error) {
	offsetBaseDir := filepath.Join(dataDir, offsetsDirName)
	if err := os.MkdirAll(offsetBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create consumer offsets directory '%s': %w", offsetBaseDir, err)
	}

	fmt.Printf("Offset Manager: Initialized. Storing offsets in %s\n", offsetBaseDir)
	return &Manager{
		baseDir: offsetBaseDir,
	}, nil
}

func (m *Manager) getOffsetFilePath(groupID, topic string, partition uint64) (string, error) {

	if strings.Contains(groupID, "..") || strings.ContainsAny(groupID, "/\\") {
		return "", fmt.Errorf("invalid characters in groupID: %s", groupID)
	}
	if strings.Contains(topic, "..") || strings.ContainsAny(topic, "/\\") {
		return "", fmt.Errorf("invalid characters in topic name: %s", topic)
	}
	if groupID == "" || topic == "" {
		return "", fmt.Errorf("groupID and topic cannot be empty")
	}

	groupDir := filepath.Join(m.baseDir, groupID)
	fileName := fmt.Sprintf("%s_%d%s", topic, partition, offsetFileExtension)
	return filepath.Join(groupDir, fileName), nil
}

func (m *Manager) Commit(groupID, topic string, partition uint64, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filePath, err := m.getOffsetFilePath(groupID, topic, partition)
	if err != nil {
		return fmt.Errorf("failed to get offset file path: %w", err)
	}

	groupDir := filepath.Dir(filePath)
	if err := os.MkdirAll(groupDir, 0755); err != nil {
		return fmt.Errorf("failed to create group directory '%s': %w", groupDir, err)
	}

	tempFilePath := filePath + tempFileSuffix
	file, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open temporary offset file '%s': %w", tempFilePath, err)
	}

	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, offset)

	_, err = file.Write(offsetBytes)
	if err != nil {
		file.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to write offset to temporary file '%s': %w", tempFilePath, err)
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to sync temporary offset file '%s': %w", tempFilePath, err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tempFilePath)
		return fmt.Errorf("failed to close temporary offset file '%s': %w", tempFilePath, err)
	}

	if err := os.Rename(tempFilePath, filePath); err != nil {

		os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary offset file '%s' to '%s': %w", tempFilePath, filePath, err)
	}

	return nil
}

func (m *Manager) Fetch(groupID, topic string, partition uint64) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	filePath, err := m.getOffsetFilePath(groupID, topic, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get offset file path: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrOffsetNotFound
		}
		return 0, fmt.Errorf("failed to open offset file '%s': %w", filePath, err)
	}
	defer file.Close()

	offsetBytes := make([]byte, 8)
	n, err := file.Read(offsetBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to read offset from file '%s': %w", filePath, err)
	}
	if n < 8 {
		return 0, fmt.Errorf("offset file '%s' is corrupted (too short)", filePath)
	}

	committedOffset := binary.BigEndian.Uint64(offsetBytes)

	return committedOffset, nil
}
