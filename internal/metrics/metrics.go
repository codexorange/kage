// Package metrics defines and registers Kage's Prometheus metrics and exposes
// the /metrics HTTP endpoint.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all instrumentation for the Kage broker.
type Metrics struct {
	MessagesProducedTotal *prometheus.CounterVec
	MessagesFetchedTotal  *prometheus.CounterVec
	DiskWriteLatency      *prometheus.HistogramVec
	reg                   *prometheus.Registry
}

// New creates a new Metrics instance with all counters and histograms
// registered in an isolated registry (not the global default).
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		MessagesProducedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kage_messages_produced_total",
				Help: "Total number of record batches successfully written to storage, labeled by topic.",
			},
			[]string{"topic"},
		),

		MessagesFetchedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kage_messages_fetched_total",
				Help: "Total number of record batches successfully served to consumers, labeled by topic.",
			},
			[]string{"topic"},
		),

		DiskWriteLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "kage_disk_write_latency_seconds",
				Help:    "Latency of storage Append calls in seconds, labeled by topic.",
				Buckets: prometheus.DefBuckets, // .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
			},
			[]string{"topic"},
		),

		reg: reg,
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.MessagesProducedTotal,
		m.MessagesFetchedTotal,
		m.DiskWriteLatency,
	)

	return m
}

// ObserveDiskWrite records the duration of one storage Append call.
// Call it with a start time captured before the Append and the topic name.
func (m *Metrics) ObserveDiskWrite(topic string, start time.Time) {
	m.DiskWriteLatency.WithLabelValues(topic).Observe(time.Since(start).Seconds())
}

// ServeHTTP starts an HTTP server on addr (e.g. "0.0.0.0:9093") exposing
// /metrics.  The server runs until ctx is cancelled, after which it performs
// a graceful shutdown with a 5-second timeout.  Errors are logged via logger.
func (m *Metrics) ServeHTTP(ctx context.Context, addr string, logger *slog.Logger) {
	m.serveHTTP(ctx, addr, logger, 5*time.Second)
}

// serveHTTP is the internal implementation of ServeHTTP with a configurable
// shutdown timeout, used by tests to exercise error paths.
func (m *Metrics) serveHTTP(ctx context.Context, addr string, logger *slog.Logger, shutdownTimeout time.Duration) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:        addr,
		Handler:     mux,
		ReadTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	}
}

// MetricsAddr returns the conventional metrics listen address for a given broker port.
// By convention, the metrics port is always broker port + 1.
func MetricsAddr(brokerPort int) string {
	return fmt.Sprintf("0.0.0.0:%d", brokerPort+1)
}
