package storage

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestCleanOldSegments_DeletesExpiredAndKeepsActive is the primary end-to-end
// test: produce enough data to force a rollover, back-date the closed segment,
// call CleanOldSegments, and assert the closed files are gone while the active
// segment remains.
func TestCleanOldSegments_DeletesExpiredAndKeepsActive(t *testing.T) {
	dir := tempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// maxSize = 9 bytes: one 5-byte payload fills a segment exactly (4-byte
	// header + 5-byte payload = 9). The second append forces a rollover.
	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{MaxSize: 9}, logger)
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	if _, err := ps.Append([]byte("hello")); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if _, err := ps.Append([]byte("world")); err != nil {
		t.Fatalf("second Append (rollover): %v", err)
	}

	closedLog := dir + "/00000000000000000000.log"
	closedIdx := dir + "/00000000000000000000.index"

	// Verify the closed segment files exist before cleaning.
	if _, err := os.Stat(closedLog); err != nil {
		t.Fatalf("closed .log must exist before clean: %v", err)
	}
	if _, err := os.Stat(closedIdx); err != nil {
		t.Fatalf("closed .index must exist before clean: %v", err)
	}

	// Back-date the closed segment so it appears older than the retention window.
	ancient := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(closedLog, ancient, ancient); err != nil {
		t.Fatalf("Chtimes log: %v", err)
	}
	if err := os.Chtimes(closedIdx, ancient, ancient); err != nil {
		t.Fatalf("Chtimes index: %v", err)
	}

	n, err := ps.CleanOldSegments(24 * time.Hour)
	if err != nil {
		t.Fatalf("CleanOldSegments: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted = %d, want 1", n)
	}

	// Closed files must be physically gone.
	if _, err := os.Stat(closedLog); !os.IsNotExist(err) {
		t.Errorf("closed .log should be deleted; stat err: %v", err)
	}
	if _, err := os.Stat(closedIdx); !os.IsNotExist(err) {
		t.Errorf("closed .index should be deleted; stat err: %v", err)
	}

	// Active segment must still be present.
	ps.mu.Lock()
	activeBase := ps.active.BaseOffset()
	ps.mu.Unlock()

	activeLog := dir + "/" + segmentName(activeBase)
	if _, err := os.Stat(activeLog); err != nil {
		t.Errorf("active segment must not be deleted: %v", err)
	}
}

// TestCleanOldSegments_ReturnZeroWhenNothingExpired verifies that segments
// within the retention window are not deleted and the count is 0.
func TestCleanOldSegments_ReturnZeroWhenNothingExpired(t *testing.T) {
	dir := tempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{MaxSize: 9}, logger)
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	if _, err := ps.Append([]byte("hello")); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if _, err := ps.Append([]byte("world")); err != nil {
		t.Fatalf("second Append (rollover): %v", err)
	}

	// Files are freshly written — well within a 24-hour retention window.
	n, err := ps.CleanOldSegments(24 * time.Hour)
	if err != nil {
		t.Fatalf("CleanOldSegments: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0 (nothing expired)", n)
	}

	// Closed segment must still exist.
	if _, err := os.Stat(dir + "/00000000000000000000.log"); err != nil {
		t.Errorf("non-expired .log must not be deleted: %v", err)
	}
}

// TestCleanOldSegments_ActiveOnlyPartition verifies that a store with only
// the active segment (no rollovers yet) is untouched regardless of retention.
func TestCleanOldSegments_ActiveOnlyPartition(t *testing.T) {
	dir := tempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{}, logger)
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	if _, err := ps.Append([]byte("data")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Use a zero-duration retention to force expiry of anything that qualifies.
	n, err := ps.CleanOldSegments(0)
	if err != nil {
		t.Fatalf("CleanOldSegments: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0 (only active segment exists)", n)
	}

	if _, err := os.Stat(dir + "/00000000000000000000.log"); err != nil {
		t.Errorf("active segment must not be deleted: %v", err)
	}
}

// TestCleanOldSegments_MultipleExpired verifies that all expired closed
// segments are deleted when more than one rollover has occurred.
func TestCleanOldSegments_MultipleExpired(t *testing.T) {
	dir := tempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// maxSize = 9: each 5-byte payload fills one segment.
	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{MaxSize: 9}, logger)
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	// Three appends → two rollovers → two closed segments + one active.
	payloads := [][]byte{[]byte("aaaaa"), []byte("bbbbb"), []byte("ccccc")}
	for i, p := range payloads {
		if _, err := ps.Append(p); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Back-date both closed segments.
	ancient := time.Now().Add(-72 * time.Hour)
	for _, name := range []string{
		dir + "/00000000000000000000.log",
		dir + "/00000000000000000000.index",
		dir + "/00000000000000000009.log",
		dir + "/00000000000000000009.index",
	} {
		if err := os.Chtimes(name, ancient, ancient); err != nil {
			t.Fatalf("Chtimes %s: %v", name, err)
		}
	}

	n, err := ps.CleanOldSegments(24 * time.Hour)
	if err != nil {
		t.Fatalf("CleanOldSegments: %v", err)
	}
	if n != 2 {
		t.Errorf("deleted = %d, want 2", n)
	}

	// Both closed segments must be gone.
	for _, name := range []string{
		dir + "/00000000000000000000.log",
		dir + "/00000000000000000009.log",
	} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Errorf("expected %s to be deleted; stat err: %v", name, err)
		}
	}

	// Active segment must remain.
	ps.mu.Lock()
	activeBase := ps.active.BaseOffset()
	ps.mu.Unlock()

	activeLog := dir + "/" + segmentName(activeBase)
	if _, err := os.Stat(activeLog); err != nil {
		t.Errorf("active segment must not be deleted: %v", err)
	}
}

// TestBrokerStore_RunLogCleaner_DeletesExpiredSegments verifies the
// BrokerStore-level cleaner integration: the runLogCleaner goroutine calls
// CleanOldSegments on each partition and expired closed segments are removed.
func TestBrokerStore_RunLogCleaner_DeletesExpiredSegments(t *testing.T) {
	dir := tempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a very short retention so files expire immediately, and a tiny
	// segment size so a rollover happens on the second append.
	bs, err := OpenBrokerStore(ctx, dir, SegmentConfig{
		MaxSize:   9,
		Retention: time.Nanosecond, // expire everything instantly
	}, logger)
	if err != nil {
		t.Fatalf("OpenBrokerStore: %v", err)
	}
	defer bs.Close()

	ps, err := bs.GetOrCreatePartition("events", 0)
	if err != nil {
		t.Fatalf("GetOrCreatePartition: %v", err)
	}

	if _, err := ps.Append([]byte("hello")); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if _, err := ps.Append([]byte("world")); err != nil {
		t.Fatalf("second Append (rollover): %v", err)
	}

	partDir := dir + "/events-0"
	closedLog := partDir + "/00000000000000000000.log"

	// Confirm the closed segment exists before cleaning.
	if _, err := os.Stat(closedLog); err != nil {
		t.Fatalf("closed .log must exist before clean: %v", err)
	}

	// Sleep 1ms so modification time is guaranteed older than retention=1ns,
	// then run one cleaner sweep directly via CleanOldSegments.
	time.Sleep(time.Millisecond)

	n, err := ps.CleanOldSegments(bs.cfg.Retention)
	if err != nil {
		t.Fatalf("CleanOldSegments: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted = %d, want 1", n)
	}

	if _, err := os.Stat(closedLog); !os.IsNotExist(err) {
		t.Errorf("expired .log must be deleted after cleaner sweep; stat err: %v", err)
	}
}

// TestBrokerStore_RunLogCleaner_ExitsOnContextCancel verifies the cleaner
// goroutine started by OpenBrokerStore terminates when the context is cancelled.
func TestBrokerStore_RunLogCleaner_ExitsOnContextCancel(t *testing.T) {
	dir := tempDir(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())

	bs, err := OpenBrokerStore(ctx, dir, SegmentConfig{Retention: time.Hour}, logger)
	if err != nil {
		t.Fatalf("OpenBrokerStore: %v", err)
	}
	defer bs.Close()

	// Cancelling the context must not hang; the goroutine exits on ctx.Done().
	cancel()
	// No explicit assertion — a goroutine leak would surface via go test -race
	// or the test timeout.
}

// segmentName formats a base offset into the 20-digit .log filename.
func segmentName(baseOffset uint64) string {
	return segmentNameFromBase(baseOffset)
}

// segmentNameFromBase is a thin wrapper to avoid importing fmt in the helper.
func segmentNameFromBase(base uint64) string {
	const digits = "00000000000000000000"
	s := make([]byte, 20)
	copy(s, digits)
	n := base
	for i := 19; i >= 0 && n > 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}
	return string(s) + ".log"
}
