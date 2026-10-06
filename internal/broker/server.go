// Package broker implements the TCP server that accepts client connections
// and dispatches requests to the store and offset manager.
package broker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"

	cfg "github.com/MX1MR41/fluxgo/internal/config"
	"github.com/MX1MR41/fluxgo/internal/offset"
	"github.com/MX1MR41/fluxgo/internal/store"
)

// Server accepts TCP connections and runs one Handler goroutine per
// connection.
type Server struct {
	config  *cfg.ServerConfig
	logger  *slog.Logger
	handler *Handler

	listener net.Listener
	quit     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	mu          sync.Mutex
	activeConns map[net.Conn]struct{}
}

// NewServer wires the store and offset manager into a server.
func NewServer(config *cfg.ServerConfig, logStore *store.Store, offManager *offset.Manager, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		config:      config,
		logger:      logger,
		handler:     NewHandler(logStore, offManager, logger, config.Server.MaxFrameBytes, config.Server.ReadTimeout, config.Server.WriteTimeout),
		quit:        make(chan struct{}),
		activeConns: make(map[net.Conn]struct{}),
	}
}

// Start listens and serves until ctx is cancelled or Stop is called.
// It returns nil on a clean shutdown.
func (s *Server) Start(ctx context.Context) error {
	addr := s.config.Server.ListenAddress
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	s.logger.Info("broker listening", "address", listener.Addr().String())

	s.wg.Add(1)
	go s.acceptLoop()

	select {
	case <-ctx.Done():
	case <-s.quit:
	}
	s.Stop()
	return nil
}

// Addr returns the listener's address, or nil before Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.logger.Warn("accept failed", "error", err)
			continue
		}

		s.mu.Lock()
		s.activeConns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.activeConns, conn)
				s.mu.Unlock()
				conn.Close()
			}()
			s.handler.Handle(conn)
		}()
	}
}

// Stop signals shutdown: the listener is closed, active connections are
// closed (unblocking in-flight reads/writes), and the call returns once all
// handler goroutines have finished. It is safe to call Stop multiple times.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.quit) })

	s.mu.Lock()
	listener := s.listener
	conns := make([]net.Conn, 0, len(s.activeConns))
	for c := range s.activeConns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if listener != nil {
		listener.Close()
	}
	for _, c := range conns {
		c.Close()
	}
	s.wg.Wait()
	s.logger.Info("broker stopped")
}
