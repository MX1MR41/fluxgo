// Package store manages the set of commit logs (one per topic-partition)
// served by the broker: loading them at startup, creating them lazily on
// first produce, and closing them on shutdown.
package store

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	clog "github.com/MX1MR41/fluxgo/internal/commitlog"
	cfg "github.com/MX1MR41/fluxgo/internal/config"
	"github.com/MX1MR41/fluxgo/internal/validate"
)

// ErrStoreClosed is returned when the store is used after Close.
var ErrStoreClosed = errors.New("store: closed")

// Store maps "topic_partition" names to their commit logs.
type Store struct {
	mu      sync.RWMutex
	baseDir string
	config  *cfg.ServerConfig
	logger  *slog.Logger
	logs    map[string]*clog.Log
	closed  bool
}

// NewStore opens the store rooted at baseDir and loads all existing logs.
func NewStore(baseDir string, config *cfg.ServerConfig, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("store: failed to create base directory %s: %w", baseDir, err)
	}
	s := &Store{
		baseDir: baseDir,
		config:  config,
		logger:  logger,
		logs:    make(map[string]*clog.Log),
	}
	if err := s.loadExistingLogs(); err != nil {
		s.Close()
		return nil, fmt.Errorf("store: failed to load existing logs: %w", err)
	}
	return s, nil
}

func (s *Store) loadExistingLogs() error {
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return err
	}
	loaded := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "__") {
			continue // broker-internal directories (e.g. __consumer_offsets)
		}
		topic, partition, err := ParseLogName(name)
		if err != nil {
			s.logger.Warn("store: skipping directory with invalid name",
				"dir", name, "error", err)
			continue
		}
		logDir := filepath.Join(s.baseDir, name)
		lg, err := clog.Open(logDir, name, s.config.GetCommitLogConfig(), s.logger)
		if err != nil {
			return fmt.Errorf("store: failed to load log %s: %w", name, err)
		}
		s.logger.Info("store: loaded log",
			"topic", topic, "partition", partition,
			"low", lg.LowestOffset(), "high", lg.HighestOffset())
		s.logs[name] = lg
		loaded++
	}
	s.logger.Info("store: finished loading", "logs", loaded)
	return nil
}

// FormatLogName builds the directory/log name for a topic-partition.
// The partition is the last "_" separated component, so topics may contain
// underscores.
func FormatLogName(topic string, partition uint32) string {
	return fmt.Sprintf("%s_%d", topic, partition)
}

// ParseLogName splits a log name into topic and partition. The split happens
// at the last underscore so topics containing underscores round-trip
// correctly (V1 split at the first underscore and broke such topics).
func ParseLogName(name string) (topic string, partition uint32, err error) {
	i := strings.LastIndexByte(name, '_')
	if i <= 0 || i == len(name)-1 {
		return "", 0, fmt.Errorf("store: log name %q does not match topic_partition", name)
	}
	p, err := strconv.ParseUint(name[i+1:], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("store: log name %q has non-numeric partition: %w", name, err)
	}
	topic = name[:i]
	if err := validate.Topic(topic); err != nil {
		return "", 0, err
	}
	return topic, uint32(p), nil
}

// GetLog returns the log for the topic-partition, or nil if it does not
// exist. It never creates anything.
func (s *Store) GetLog(topic string, partition uint32) *clog.Log {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}
	return s.logs[FormatLogName(topic, partition)]
}

// GetOrCreateLog returns the log for the topic-partition, creating (and
// persisting) it on first use. Topic names are validated before touching
// disk.
func (s *Store) GetOrCreateLog(topic string, partition uint32) (*clog.Log, error) {
	if err := validate.Topic(topic); err != nil {
		return nil, err
	}
	name := FormatLogName(topic, partition)

	s.mu.RLock()
	lg, ok := s.logs[name]
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, ErrStoreClosed
	}
	if ok {
		return lg, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	if lg, ok := s.logs[name]; ok { // another goroutine won the race
		return lg, nil
	}

	logDir := filepath.Join(s.baseDir, name)
	lg, err := clog.Open(logDir, name, s.config.GetCommitLogConfig(), s.logger)
	if err != nil {
		return nil, fmt.Errorf("store: failed to create log %s: %w", name, err)
	}
	s.logs[name] = lg
	s.logger.Info("store: created log", "topic", topic, "partition", partition)
	return lg, nil
}

// Topics returns the sorted names of all known topics.
func (s *Store) Topics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := make(map[string]struct{}, len(s.logs))
	for name := range s.logs {
		if topic, _, err := ParseLogName(name); err == nil {
			set[topic] = struct{}{}
		}
	}
	topics := make([]string, 0, len(set))
	for t := range set {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	return topics
}

// Partitions returns the sorted partition IDs of a topic, or nil if the
// topic is unknown.
func (s *Store) Partitions(topic string) []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var partitions []uint32
	for name := range s.logs {
		t, p, err := ParseLogName(name)
		if err == nil && t == topic {
			partitions = append(partitions, p)
		}
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions
}

// Close closes every open log. The store must not be used afterwards.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	for name, lg := range s.logs {
		if err := lg.Close(); err != nil {
			s.logger.Error("store: failed to close log", "log", name, "error", err)
			errs = append(errs, err)
		}
		delete(s.logs, name)
	}
	return errors.Join(errs...)
}
