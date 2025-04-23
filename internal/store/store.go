package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	clog "github.com/MX1MR41/fluxgo/internal/commitlog"
	cfg "github.com/MX1MR41/fluxgo/internal/config"
)

type Store struct {
	mu      sync.RWMutex
	baseDir string
	config  *cfg.ServerConfig
	logs    map[string]clog.Log

	closed bool
}

func NewStore(baseDir string, config *cfg.ServerConfig) (*Store, error) {

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base store directory %s: %w", baseDir, err)
	}

	s := &Store{
		baseDir: baseDir,
		config:  config,
		logs:    make(map[string]clog.Log),
	}

	if err := s.loadExistingLogs(); err != nil {

		s.Close()
		return nil, fmt.Errorf("failed during initial log loading: %w", err)
	}

	return s, nil
}

func (s *Store) loadExistingLogs() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("Store: Loading existing logs from %s...\n", s.baseDir)

	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return fmt.Errorf("failed to read base store directory %s: %w", s.baseDir, err)
	}

	loadedCount := 0
	for _, entry := range entries {

		if !entry.IsDir() {
			continue
		}

		logName := entry.Name()
		logDir := filepath.Join(s.baseDir, logName)

		parts := strings.SplitN(logName, "_", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "Store: Skipping directory '%s' in %s - invalid name format (expected 'topic_partition')\n", logName, s.baseDir)
			continue
		}
		topic := parts[0]
		partitionStr := parts[1]

		fmt.Printf("Store: Found potential log directory: %s (Topic: %s, Partition: %s)\n", logDir, topic, partitionStr)

		commitLogConfig := s.config.GetCommitLogConfig(logDir)

		logInstance, err := clog.NewLog(logDir, logName, commitLogConfig)
		if err != nil {

			return fmt.Errorf("failed to load log '%s' from %s: %w", logName, logDir, err)
		}

		fmt.Printf("Store: Successfully loaded log '%s' (Lowest: %d, Highest: %d)\n",
			logInstance.Name(), logInstance.LowestOffset(), logInstance.HighestOffset())

		s.logs[logName] = logInstance
		loadedCount++
	}

	fmt.Printf("Store: Finished loading. Loaded %d logs.\n", loadedCount)
	return nil
}

func (s *Store) GetLog(topic string, partition uint64) clog.Log {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil
	}

	logName := formatLogName(topic, partition)
	logInstance, ok := s.logs[logName]
	if !ok {
		return nil
	}
	return logInstance
}

func (s *Store) GetOrCreateLog(topic string, partition uint64) (clog.Log, error) {
	logName := formatLogName(topic, partition)

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, fmt.Errorf("store is closed")
	}
	logInstance, ok := s.logs[logName]
	s.mu.RUnlock()

	if ok {
		return logInstance, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	logInstance, ok = s.logs[logName]
	if ok {
		return logInstance, nil
	}

	fmt.Printf("Store: Creating new log for Topic: %s, Partition: %d\n", topic, partition)
	logDir := filepath.Join(s.baseDir, logName)

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for log %s: %w", logName, err)
	}

	commitLogConfig := s.config.GetCommitLogConfig(logDir)

	newLogInstance, err := clog.NewLog(logDir, logName, commitLogConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create new log %s: %w", logName, err)
	}

	s.logs[logName] = newLogInstance
	fmt.Printf("Store: Successfully created log '%s'\n", newLogInstance.Name())

	return newLogInstance, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	fmt.Printf("Store: Closing %d logs...\n", len(s.logs))
	var closeErrors []string
	for name, logInstance := range s.logs {
		if err := logInstance.Close(); err != nil {
			errMsg := fmt.Sprintf("failed to close log '%s': %v", name, err)
			fmt.Fprintf(os.Stderr, "Error: %s\n", errMsg)
			closeErrors = append(closeErrors, errMsg)
		}

		delete(s.logs, name)
	}

	if len(closeErrors) > 0 {
		return fmt.Errorf("encountered errors while closing logs: %s", strings.Join(closeErrors, "; "))
	}

	fmt.Println("Store: All logs closed.")
	return nil
}

func formatLogName(topic string, partition uint64) string {

	return fmt.Sprintf("%s_%d", topic, partition)
}

func (s *Store) ListTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil
	}

	topicSet := make(map[string]struct{})
	for logName := range s.logs {
		parts := strings.SplitN(logName, "_", 2)
		if len(parts) == 2 {
			topicSet[parts[0]] = struct{}{}
		}
	}

	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	return topics
}

func (s *Store) ListPartitions(topic string) ([]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, io.ErrClosedPipe
	}

	partitions := make([]uint64, 0)
	prefix := topic + "_"

	for logName := range s.logs {
		if strings.HasPrefix(logName, prefix) {
			partitionStr := strings.TrimPrefix(logName, prefix)
			partitionID, err := strconv.ParseUint(partitionStr, 10, 64)
			if err == nil {
				partitions = append(partitions, partitionID)
			} else {

				fmt.Fprintf(os.Stderr, "Warning: Found log '%s' with unexpected partition format for topic '%s'\n", logName, topic)
			}
		}
	}

	if len(partitions) == 0 {

		topicExists := false
		for logName := range s.logs {
			if strings.HasPrefix(logName, prefix) {
				topicExists = true
				break
			}
		}
		if !topicExists {

			return nil, fmt.Errorf("topic '%s' not found", topic)
		}
	}

	return partitions, nil
}
