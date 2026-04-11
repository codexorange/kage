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

	"github.com/codexorange/kage/internal/config"
	"github.com/codexorange/kage/internal/server"
	"github.com/codexorange/kage/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load(config.DefaultConfigFile)
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger.Info("configuration loaded",
		"port", cfg.Port,
		"log_directory", cfg.LogDirectory,
		"max_segment_size", cfg.MaxSegmentSize,
		"worker_pool_size", cfg.WorkerPoolSize,
		"shutdown_timeout", cfg.ShutdownTimeout,
	)

	// Root context cancelled on SIGTERM/SIGINT.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(cfg.LogDirectory, 0o755); err != nil {
		logger.Error("failed to create log directory", "path", cfg.LogDirectory, "error", err)
		os.Exit(1)
	}

	store, err := storage.OpenPartitionStore(cfg.LogDirectory, storage.SegmentConfig{
		MaxSize: cfg.MaxSegmentSize,
	})
	if err != nil {
		logger.Error("failed to open partition store", "dir", cfg.LogDirectory, "error", err)
		os.Exit(1)
	}
	defer store.Close()

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", cfg.Addr())
	if err != nil {
		logger.Error("failed to bind listener", "error", err, "addr", cfg.Addr())
		os.Exit(1)
	}

	logger.Info("Kage broker started",
		"address", cfg.Addr(),
		"log_directory", cfg.LogDirectory,
		"pid", os.Getpid(),
		"worker_pool_size", cfg.WorkerPoolSize,
	)

	handler := server.NewHandler(logger, store)

	sem := make(chan struct{}, cfg.WorkerPoolSize)
	var wg sync.WaitGroup

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
					"worker_pool_size", cfg.WorkerPoolSize,
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
	case <-time.After(cfg.ShutdownTimeout):
		logger.Warn("shutdown timeout exceeded, forcing exit", "timeout", cfg.ShutdownTimeout)
	}
}
