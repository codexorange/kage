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
)

const (
	defaultAddr        = "0.0.0.0:9092"
	maxConcurrentConns = 10_000
	shutdownTimeout    = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := defaultAddr

	// Root context cancelled on SIGTERM/SIGINT.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		logger.Error("failed to bind listener", "error", err, "addr", addr)
		os.Exit(1)
	}

	logger.Info("Kage broker started", "address", addr, "pid", os.Getpid(), "max_conns", maxConcurrentConns)

	handler := server.NewHandler(logger)

	// Semaphore: limits concurrent active connections.
	sem := make(chan struct{}, maxConcurrentConns)

	// WaitGroup tracks all active connection goroutines.
	var wg sync.WaitGroup

	// Accept loop runs until the listener is closed.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Listener was closed — normal shutdown path.
				if errors.Is(err, net.ErrClosed) {
					return
				}
				logger.Error("accept error", "error", err)
				return
			}

			// Acquire a slot in the worker pool. If the pool is full, the new
			// connection is rejected immediately to apply back-pressure.
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
					<-sem // release slot back to pool
					wg.Done()
				}()
				handler.Handle(c)
			}(conn)
		}
	}()

	// Block until signal is received.
	<-ctx.Done()
	logger.Info("shutdown signal received, closing listener")

	// Stop accepting new connections.
	listener.Close()

	// Wait for active workers with a hard deadline.
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
