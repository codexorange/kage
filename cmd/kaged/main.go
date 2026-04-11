package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/codexorange/kage/internal/server"
	"github.com/codexorange/kage/internal/storage"
)

const (
	defaultAddr        = "0.0.0.0:9092"
	defaultDataDir     = "/data"
	maxConcurrentConns = 10_000
	shutdownTimeout    = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Root context cancelled on SIGTERM/SIGINT.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Ensure the data directory exists.
	if err := os.MkdirAll(defaultDataDir, 0o755); err != nil {
		logger.Error("failed to create data dir", "path", defaultDataDir, "error", err)
		os.Exit(1)
	}

	// Open the single partition store backed by the data directory.
	store, err := storage.OpenPartitionStore(defaultDataDir, storage.SegmentConfig{})
	if err != nil {
		logger.Error("failed to open partition store", "dir", defaultDataDir, "error", err)
		os.Exit(1)
	}
	defer store.Close()

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", defaultAddr)
	if err != nil {
		logger.Error("failed to bind listener", "error", err, "addr", defaultAddr)
		os.Exit(1)
	}

	logger.Info("Kage broker started",
		"address", defaultAddr,
		"data_dir", defaultDataDir,
		"pid", os.Getpid(),
		"max_conns", maxConcurrentConns,
	)

	handler := server.NewHandler(logger, store)

	// Semaphore: limits concurrent active connections.
	sem := make(chan struct{}, maxConcurrentConns)

	// WaitGroup tracks all active connection goroutines.
	var wg sync.WaitGroup

	// Accept loop runs until the listener is closed.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				logger.Error("accept error", "error", err)
				return
			}

			select {
			case sem <- struct{}{}:
			default:
				logger.Warn("worker pool exhausted, rejecting connection",
					"client", conn.RemoteAddr().String(),
					"max_conns", maxConcurrentConns,
				)
				conn.Close()
				continue
			}

			wg.Add(1)
			go func(c net.Conn) {
				defer func() {
					<-sem
					wg.Done()
				}()
				handler.Handle(c)
			}(conn)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, closing listener")

	listener.Close()

	shutdownDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		logger.Info("all connections drained, Kage stopped cleanly")
	case <-time.After(shutdownTimeout):
		logger.Warn("shutdown timeout exceeded, forcing exit", "timeout", shutdownTimeout)
	}
}
