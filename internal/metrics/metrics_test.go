package metrics

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// freeAddr binds a listener on :0, records the assigned port, closes the
// listener, and returns "127.0.0.1:<port>".  There is a tiny race between
// close and the test server's bind, but it is negligible in practice.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// gather starts m's HTTP server on a random port, fetches /metrics, shuts
// the server down, and returns the response body as a string.
func gather(t *testing.T, m *Metrics) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := freeAddr(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go m.ServeHTTP(ctx, addr, logger)

	url := "http://" + addr + "/metrics"
	for i := 0; i < 30; i++ {
		resp, err := http.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return string(body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("metrics server did not become ready")
	return ""
}

// assertContains fails the test if substr is not present in s.
func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected output to contain %q\nfull output:\n%s", substr, s)
	}
}

// ── New / registry isolation ──────────────────────────────────────────────────

func TestNew_RegistersMetrics(t *testing.T) {
	m := New()
	if m.MessagesProducedTotal == nil {
		t.Error("MessagesProducedTotal is nil")
	}
	if m.MessagesFetchedTotal == nil {
		t.Error("MessagesFetchedTotal is nil")
	}
	if m.DiskWriteLatency == nil {
		t.Error("DiskWriteLatency is nil")
	}
}

func TestNew_IsolatedRegistry(t *testing.T) {
	// Each New() call must succeed without panicking (no global registration conflicts).
	m1 := New()
	m2 := New()
	if m1.reg == m2.reg {
		t.Error("expected independent registries, got the same pointer")
	}
}

// ── Counter increments ────────────────────────────────────────────────────────

func TestMessagesProducedTotal_Increments(t *testing.T) {
	m := New()
	m.MessagesProducedTotal.WithLabelValues("kage-events").Inc()
	m.MessagesProducedTotal.WithLabelValues("kage-events").Inc()
	m.MessagesProducedTotal.WithLabelValues("other-topic").Inc()

	body := gather(t, m)
	assertContains(t, body, `kage_messages_produced_total{topic="kage-events"} 2`)
	assertContains(t, body, `kage_messages_produced_total{topic="other-topic"} 1`)
}

func TestMessagesFetchedTotal_Increments(t *testing.T) {
	m := New()
	m.MessagesFetchedTotal.WithLabelValues("kage-events").Inc()

	body := gather(t, m)
	assertContains(t, body, `kage_messages_fetched_total{topic="kage-events"} 1`)
}

func TestCounters_StartsAtZero(t *testing.T) {
	m := New()
	// Trigger label creation without incrementing.
	m.MessagesProducedTotal.WithLabelValues("t")
	body := gather(t, m)
	assertContains(t, body, `kage_messages_produced_total{topic="t"} 0`)
}

// ── Histogram ─────────────────────────────────────────────────────────────────

func TestObserveDiskWrite_RecordsObservation(t *testing.T) {
	m := New()
	start := time.Now().Add(-10 * time.Millisecond)
	m.ObserveDiskWrite("kage-events", start)

	body := gather(t, m)
	assertContains(t, body, `kage_disk_write_latency_seconds_count{topic="kage-events"} 1`)
	assertContains(t, body, `kage_disk_write_latency_seconds_sum{topic="kage-events"}`)
}

func TestObserveDiskWrite_MultipleTopics(t *testing.T) {
	m := New()
	start := time.Now()
	m.ObserveDiskWrite("topic-a", start)
	m.ObserveDiskWrite("topic-a", start)
	m.ObserveDiskWrite("topic-b", start)

	body := gather(t, m)
	assertContains(t, body, `kage_disk_write_latency_seconds_count{topic="topic-a"} 2`)
	assertContains(t, body, `kage_disk_write_latency_seconds_count{topic="topic-b"} 1`)
}

func TestObserveDiskWrite_PositiveDuration(t *testing.T) {
	m := New()
	// Observe a zero-duration write (start == now).
	m.ObserveDiskWrite("t", time.Now())
	body := gather(t, m)
	assertContains(t, body, `kage_disk_write_latency_seconds_count{topic="t"} 1`)
}

// ── MetricsAddr ───────────────────────────────────────────────────────────────

func TestMetricsAddr(t *testing.T) {
	cases := []struct {
		brokerPort int
		want       string
	}{
		{9092, "0.0.0.0:9093"},
		{8080, "0.0.0.0:8081"},
		{1000, "0.0.0.0:1001"},
	}
	for _, c := range cases {
		got := MetricsAddr(c.brokerPort)
		if got != c.want {
			t.Errorf("MetricsAddr(%d) = %q, want %q", c.brokerPort, got, c.want)
		}
	}
}

// ── HTTP endpoint ─────────────────────────────────────────────────────────────

func TestServeHTTP_ExposesMetricsEndpoint(t *testing.T) {
	m := New()
	m.MessagesProducedTotal.WithLabelValues("test-topic").Add(3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr := freeAddr(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go m.ServeHTTP(ctx, addr, logger)

	url := fmt.Sprintf("http://%s/metrics", addr)
	var body string
	for i := 0; i < 30; i++ {
		resp, err := http.Get(url)
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = string(raw)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if body == "" {
		t.Fatal("metrics server did not become ready")
	}
	assertContains(t, body, "kage_messages_produced_total")
	assertContains(t, body, `topic="test-topic"`)
}

func TestServeHTTP_GracefulShutdown(t *testing.T) {
	m := New()
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	addr := freeAddr(t)
	done := make(chan struct{})
	go func() {
		m.ServeHTTP(ctx, addr, logger)
		close(done)
	}()

	// Wait until the server is up.
	for i := 0; i < 30; i++ {
		if _, err := http.Get(fmt.Sprintf("http://%s/metrics", addr)); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not return after context cancellation")
	}
}

func TestServeHTTP_GoAndProcessCollectors(t *testing.T) {
	m := New()
	body := gather(t, m)
	// The Go and process collectors should be present.
	assertContains(t, body, "go_goroutines")
	assertContains(t, body, "process_")
}

// TestServeHTTP_BadAddr covers the ListenAndServe error path: an invalid
// address makes the server goroutine log an error and exit.
func TestServeHTTP_BadAddr(t *testing.T) {
	m := New()
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{})

	go func() {
		// "invalid:::addr" causes ListenAndServe to return an error immediately.
		m.serveHTTP(ctx, "invalid:::addr", logger, 100*time.Millisecond)
		close(done)
	}()

	// Give the server goroutine time to hit the error, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serveHTTP did not return")
	}
}

// TestServeHTTP_ShutdownError covers the Shutdown error path: using a
// zero-duration shutdown timeout forces the context to be already cancelled
// when Shutdown is called, causing it to return an error.
func TestServeHTTP_ShutdownError(t *testing.T) {
	m := New()
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	addr := freeAddr(t)
	done := make(chan struct{})
	go func() {
		// shutdownTimeout = 0 means the shutdown context is immediately expired.
		m.serveHTTP(ctx, addr, logger, 0)
		close(done)
	}()

	// Wait for the server to be up.
	for i := 0; i < 30; i++ {
		if _, err := http.Get(fmt.Sprintf("http://%s/metrics", addr)); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serveHTTP did not return after cancel with zero shutdown timeout")
	}
}
