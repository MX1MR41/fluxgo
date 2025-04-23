package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	broker "github.com/MX1MR41/fluxgo/internal/broker"
	cfg "github.com/MX1MR41/fluxgo/internal/config"
	offset "github.com/MX1MR41/fluxgo/internal/offset"
	store "github.com/MX1MR41/fluxgo/internal/store"
)

var (
	configFile = flag.String("config", "configs/server.yaml", "Path to the server configuration file.")
)

func main() {
	flag.Parse()

	config, err := cfg.LoadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	if err := config.EnsureDataDir(); err != nil {
		fmt.Fprintf(os.Stderr, "Error ensuring data directory: %v\n", err)
		os.Exit(1)
	}
	absDataDir, _ := filepath.Abs(config.Log.DataDir)

	logStore, err := store.NewStore(absDataDir, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing store in %s: %v\n", absDataDir, err)
		os.Exit(1)
	}

	defer func() {
		fmt.Println("Closing store...")
		if err := logStore.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing store: %v\n", err)
		}
	}()

	offsetManager, err := offset.NewManager(absDataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing offset manager in %s: %v\n", absDataDir, err)
		os.Exit(1)
	}

	srv, err := broker.NewServer(config, logStore, offsetManager)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating server: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.Start(ctx)
	}()

	select {
	case err := <-serverErr:
		if err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)

		} else {
			fmt.Println("Server stopped.")
		}
	case <-ctx.Done():
		fmt.Println("Shutdown signal received, waiting for server to stop...")
		err := <-serverErr
		if err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "Server exited with error during shutdown: %v\n", err)
		}
		fmt.Println("Server shutdown complete.")
	}

	fmt.Println("FluxGo server exiting.")

}
