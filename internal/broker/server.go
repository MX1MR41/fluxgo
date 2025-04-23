package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	cfg "github.com/MX1MR41/fluxgo/internal/config"
	offset "github.com/MX1MR41/fluxgo/internal/offset"
	store "github.com/MX1MR41/fluxgo/internal/store"
)

type Server struct {
	config        *cfg.ServerConfig
	listener      net.Listener
	quit          chan struct{}
	wg            sync.WaitGroup
	mu            sync.RWMutex
	store         *store.Store
	offsetManager *offset.Manager
	handler       *Handler
	activeConns   map[net.Conn]struct{}
}

func NewServer(config *cfg.ServerConfig, logStore *store.Store, offManager *offset.Manager) (*Server, error) {

	handler := NewHandler(logStore, offManager)

	return &Server{
		config:        config,
		quit:          make(chan struct{}),
		store:         logStore,
		offsetManager: offManager,
		handler:       handler,
		activeConns:   make(map[net.Conn]struct{}),
	}, nil
}

func (s *Server) Start(ctx context.Context) error {
	addr := s.config.Server.ListenAddress
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener
	fmt.Printf("FluxGo broker listening on %s\n", listener.Addr())

	s.wg.Add(1)
	go s.acceptLoop()

	select {
	case <-ctx.Done():
		fmt.Println("Shutdown signal received via context...")
		s.Stop()

		return nil
	case <-s.quit:
		fmt.Println("Shutdown signal received via Stop()...")
		return nil
	}
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				fmt.Println("Accept loop shutting down.")
				return
			default:

				if !errors.Is(err, net.ErrClosed) {
					fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
				}

				continue
			}
		}

		s.mu.Lock()
		s.activeConns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)

		go func() {

			s.handler.Handle(conn, s.config.Server.ReadTimeout, s.config.Server.WriteTimeout)

			s.mu.Lock()
			delete(s.activeConns, conn)
			s.mu.Unlock()
			conn.Close()
			s.wg.Done()
		}()
	}
}

func (s *Server) Stop() {

	select {
	case <-s.quit:

		return
	default:
		close(s.quit)
	}

	if s.listener != nil {
		s.listener.Close()
	}

	s.mu.RLock()
	connsToClose := make([]net.Conn, 0, len(s.activeConns))
	for conn := range s.activeConns {
		connsToClose = append(connsToClose, conn)
	}
	s.mu.RUnlock()

	fmt.Printf("Shutting down %d active connections...\n", len(connsToClose))
	for _, conn := range connsToClose {

		conn.Close()
	}

	fmt.Println("Waiting for handlers and accept loop to finish...")
	s.wg.Wait()
	fmt.Println("FluxGo broker server shut down gracefully.")
}
