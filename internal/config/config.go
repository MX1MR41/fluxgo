package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	clog "github.com/MX1MR41/fluxgo/internal/commitlog"
	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Server ServerSettings `yaml:"server"`
	Log    LogSettings    `yaml:"log"`
}

type ServerSettings struct {
	ListenAddress string        `yaml:"listen_address"`
	ReadTimeout   time.Duration `yaml:"read_timeout"`
	WriteTimeout  time.Duration `yaml:"write_timeout"`
}

type LogSettings struct {
	DataDir         string `yaml:"data_dir"`
	MaxSegmentBytes int64  `yaml:"max_segment_bytes"`
	MaxLogBytes     int64  `yaml:"max_log_bytes"`
	FileSync        bool   `yaml:"file_sync"`
}

func LoadConfig(configPath string) (*ServerConfig, error) {

	config := &ServerConfig{
		Server: ServerSettings{
			ListenAddress: "127.0.0.1:9898",
			ReadTimeout:   30 * time.Second,
			WriteTimeout:  30 * time.Second,
		},
		Log: LogSettings{
			DataDir:         "./fluxgo-data/",
			MaxSegmentBytes: 1024 * 1024 * 16,
			MaxLogBytes:     1024 * 1024 * 1024,
			FileSync:        true,
		},
	}

	configFile, err := os.ReadFile(configPath)
	if err != nil {

		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	err = yaml.Unmarshal(configFile, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file '%s': %w", configPath, err)
	}

	config.Log.DataDir, err = filepath.Abs(config.Log.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for data_dir '%s': %w", config.Log.DataDir, err)
	}

	fmt.Printf("Configuration loaded successfully from %s\n", configPath)
	fmt.Printf("  Listen Address: %s\n", config.Server.ListenAddress)
	fmt.Printf("  Data Directory: %s\n", config.Log.DataDir)

	return config, nil
}

func (c *ServerConfig) EnsureDataDir() error {
	if c.Log.DataDir == "" {
		return fmt.Errorf("log data directory is not configured")
	}
	err := os.MkdirAll(c.Log.DataDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create data directory '%s': %w", c.Log.DataDir, err)
	}
	return nil
}

func (c *ServerConfig) GetCommitLogConfig(logSpecificDir string) clog.Config {
	return clog.Config{
		Path:            logSpecificDir,
		MaxSegmentBytes: c.Log.MaxSegmentBytes,
		MaxLogBytes:     c.Log.MaxLogBytes,
		FileSync:        c.Log.FileSync,
	}
}
