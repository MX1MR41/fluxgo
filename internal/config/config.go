// Package config loads and validates the broker's YAML configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	clog "github.com/MX1MR41/fluxgo/internal/commitlog"
	"gopkg.in/yaml.v3"
)

// ServerConfig is the root of the configuration file.
type ServerConfig struct {
	Server ServerSettings `yaml:"server"`
	Log    LogSettings    `yaml:"log"`
}

// ServerSettings controls networking behavior.
type ServerSettings struct {
	ListenAddress string        `yaml:"listen_address"`
	ReadTimeout   time.Duration `yaml:"read_timeout"`
	WriteTimeout  time.Duration `yaml:"write_timeout"`
	// MaxFrameBytes bounds a single request frame (and therefore a single
	// produced message or fetch response).
	MaxFrameBytes int64 `yaml:"max_frame_bytes"`
}

// LogSettings controls storage behavior.
type LogSettings struct {
	DataDir         string `yaml:"data_dir"`
	MaxSegmentBytes int64  `yaml:"max_segment_bytes"`
	MaxLogBytes     int64  `yaml:"max_log_bytes"`
	FileSync        bool   `yaml:"file_sync"`
}

func defaults() *ServerConfig {
	return &ServerConfig{
		Server: ServerSettings{
			ListenAddress: "127.0.0.1:9898",
			ReadTimeout:   60 * time.Second,
			WriteTimeout:  60 * time.Second,
			MaxFrameBytes: 16 * 1024 * 1024,
		},
		Log: LogSettings{
			DataDir:         "./fluxgo-data",
			MaxSegmentBytes: 32 * 1024 * 1024,
			MaxLogBytes:     2 * 1024 * 1024 * 1024,
			FileSync:        true,
		},
	}
}

// LoadConfig reads the YAML file at configPath over the built-in defaults
// and validates the result.
func LoadConfig(configPath string) (*ServerConfig, error) {
	config := defaults()

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read %s: %w", configPath, err)
	}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("config: failed to parse %s: %w", configPath, err)
	}

	if config.Log.DataDir, err = filepath.Abs(config.Log.DataDir); err != nil {
		return nil, fmt.Errorf("config: invalid data_dir %q: %w", config.Log.DataDir, err)
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return config, nil
}

// Validate checks the configuration for values that cannot work.
func (c *ServerConfig) Validate() error {
	if c.Server.ListenAddress == "" {
		return fmt.Errorf("server.listen_address must not be empty")
	}
	if c.Server.ReadTimeout < 0 || c.Server.WriteTimeout < 0 {
		return fmt.Errorf("server timeouts must not be negative")
	}
	const minFrame, maxFrame = 1024, 1 << 30
	if c.Server.MaxFrameBytes < minFrame || c.Server.MaxFrameBytes > maxFrame {
		return fmt.Errorf("server.max_frame_bytes must be between %d and %d", minFrame, maxFrame)
	}
	if c.Log.DataDir == "" {
		return fmt.Errorf("log.data_dir must not be empty")
	}
	if c.Log.MaxSegmentBytes <= 0 {
		return fmt.Errorf("log.max_segment_bytes must be positive")
	}
	if c.Log.MaxLogBytes < 0 {
		return fmt.Errorf("log.max_log_bytes must not be negative (0 disables retention)")
	}
	if c.Log.MaxLogBytes > 0 && c.Log.MaxLogBytes < c.Log.MaxSegmentBytes {
		return fmt.Errorf("log.max_log_bytes (%d) must be >= log.max_segment_bytes (%d)",
			c.Log.MaxLogBytes, c.Log.MaxSegmentBytes)
	}
	return nil
}

// EnsureDataDir creates the data directory if needed.
func (c *ServerConfig) EnsureDataDir() error {
	if err := os.MkdirAll(c.Log.DataDir, 0o755); err != nil {
		return fmt.Errorf("config: failed to create data directory %s: %w", c.Log.DataDir, err)
	}
	return nil
}

// GetCommitLogConfig derives the commitlog configuration for one log.
func (c *ServerConfig) GetCommitLogConfig() clog.Config {
	return clog.Config{
		MaxSegmentBytes: c.Log.MaxSegmentBytes,
		MaxLogBytes:     c.Log.MaxLogBytes,
		FileSync:        c.Log.FileSync,
	}
}
