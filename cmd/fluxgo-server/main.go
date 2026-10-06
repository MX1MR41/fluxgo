// Command fluxgo-server runs the FluxGo broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/MX1MR41/fluxgo/internal/broker"
	cfg "github.com/MX1MR41/fluxgo/internal/config"
	"github.com/MX1MR41/fluxgo/internal/offset"
	"github.com/MX1MR41/fluxgo/internal/store"
)

func main() {
	configPath := flag.String("config", "configs/server.yaml", "path to the server configuration file")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	config, err := cfg.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if err := config.EnsureDataDir(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	logger.Info("configuration loaded",
		"listenAddress", config.Server.ListenAddress,
		"dataDir", config.Log.DataDir,
		"maxSegmentBytes", config.Log.MaxSegmentBytes,
		"maxLogBytes", config.Log.MaxLogBytes,
		"fileSync", config.Log.FileSync,
	)

	logStore, err := store.NewStore(config.Log.DataDir, config, logger)
	if err != nil {
		logger.Error("failed to initialize store", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := logStore.Close(); err != nil {
			logger.Error("failed to close store", "error", err)
		}
	}()

	offsetManager, err := offset.NewManager(config.Log.DataDir, logger)
	if err != nil {
		logger.Error("failed to initialize offset manager", "error", err)
		os.Exit(1)
	}

	srv := broker.NewServer(config, logStore, offsetManager, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Start(ctx); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}
