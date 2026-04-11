package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// AppendStore is the interface the server layer uses to persist record batches.
// It is satisfied by *PartitionStore and can be faked in tests.
type AppendStore interface {
	// Append writes the raw RecordBatch bytes and returns the byte offset at
	// which the record was written.
	Append(data []byte) (offset uint64, err error)
}

// FetchStore is the interface the server layer uses to read record batches.
// It is satisfied by *PartitionStore and can be faked in tests.
type FetchStore interface {
	// Read returns an io.Reader covering [fetchOffset, fetchOffset+n) where n
	// is the actual byte count (≤ maxBytes, capped to available data).
	// n is returned alongside the reader so callers can write the batch-size
	// field without buffering the payload.
	// Returns ErrInvalidOffset when fetchOffset is beyond the written data.
	Read(fetchOffset uint64, maxBytes int32) (io.Reader, int32, error)

	// Size returns the total bytes written across all segments (high-watermark).
	Size() int64
}

// Store combines read and write access to a single partition's log.
type Store interface {
	AppendStore
	FetchStore
}

// PartitionStore manages the active Segment for a single topic-partition.
// It handles automatic roll-over when the active segment is full.
//
// PartitionStore is safe for concurrent use.
type PartitionStore struct {
	mu      sync.Mutex
	dir     string
	cfg     SegmentConfig
	active  *Segment
	logger  *slog.Logger
	// nextBase is the logical offset for the next segment, set to the byte
	// size of the current segment when it rolls over.
	nextBase uint64
}

// cleanerInterval is how often the log cleaner wakes to scan for expired segments.
const cleanerInterval = time.Minute

// OpenPartitionStore opens (or creates) a PartitionStore rooted at dir.
// It always starts with base offset 0 on the first segment.
// If cfg.Retention > 0, a background log-cleaner goroutine is started; it
// stops when ctx is cancelled.
func OpenPartitionStore(ctx context.Context, dir string, cfg SegmentConfig, logger *slog.Logger) (*PartitionStore, error) {
	seg, err := OpenSegment(dir, 0, cfg)
	if err != nil {
		return nil, fmt.Errorf("storage: open partition store: %w", err)
	}
	ps := &PartitionStore{
		dir:    dir,
		cfg:    cfg,
		active: seg,
		logger: logger,
	}
	if cfg.Retention > 0 {
		go ps.startCleaner(ctx)
	}
	return ps, nil
}

// Append writes data to the active segment, rolling over to a new segment
// if the current one is full. Returns the absolute byte offset of the record.
func (ps *PartitionStore) Append(data []byte) (uint64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	offset, err := ps.active.Append(data)
	if err == ErrSegmentFull {
		if err := ps.rollover(); err != nil {
			return 0, err
		}
		offset, err = ps.active.Append(data)
	}
	if err != nil {
		return 0, fmt.Errorf("storage: partition store append: %w", err)
	}
	return offset, nil
}

// rollover closes the current segment and opens a fresh one.
// Must be called with ps.mu held.
func (ps *PartitionStore) rollover() error {
	size := uint64(ps.active.Size())
	if err := ps.active.Close(); err != nil {
		return fmt.Errorf("storage: rollover close: %w", err)
	}
	ps.nextBase += size
	seg, err := OpenSegment(ps.dir, ps.nextBase, ps.cfg)
	if err != nil {
		return fmt.Errorf("storage: rollover open: %w", err)
	}
	ps.active = seg
	return nil
}

// Read returns an io.Reader for the record batch starting at fetchOffset,
// capped to maxBytes.  It reads from the active segment only; multi-segment
// reads are not yet supported (the active segment holds the full log for now).
//
// fetchOffset is the byte offset returned by a prior Append call.
// maxBytes caps how many bytes are served; it must be > 0.
func (ps *PartitionStore) Read(fetchOffset uint64, maxBytes int32) (io.Reader, int32, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	segSize := ps.active.Size() // total bytes written (includes buffered data)
	if int64(fetchOffset) >= segSize {
		return nil, 0, ErrInvalidOffset
	}

	// Cap the read window: don't read past the end of written data.
	available := int64(segSize) - int64(fetchOffset)
	if int64(maxBytes) > available {
		maxBytes = int32(available)
	}

	r, err := ps.active.Read(fetchOffset, maxBytes)
	if err != nil {
		return nil, 0, err
	}
	return r, maxBytes, nil
}

// Size returns the total bytes written to the active segment (high-watermark).
func (ps *PartitionStore) Size() int64 {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.active.Size()
}

// Flush commits buffered writes to the OS for the active segment.
func (ps *PartitionStore) Flush() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.active.Flush()
}

// Close flushes and closes the active segment.
func (ps *PartitionStore) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.active.Close()
}

// startCleaner runs a periodic log-retention sweep in a background goroutine.
// It deletes closed segment files (.log and .index) whose last-modification
// time is older than cfg.Retention. The active segment is never deleted.
// The goroutine exits when ctx is cancelled.
func (ps *PartitionStore) startCleaner(ctx context.Context) {
	ticker := time.NewTicker(cleanerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ps.clean()
		}
	}
}

// clean performs one retention sweep. It is safe to call concurrently with
// Append and Read because it only reads ps.active.BaseOffset() under no lock
// (BaseOffset is immutable after open) and uses the filesystem as its source
// of truth for closed segments.
func (ps *PartitionStore) clean() {
	// Snapshot the active segment's base offset so we never delete it.
	ps.mu.Lock()
	activeBase := ps.active.BaseOffset()
	ps.mu.Unlock()

	activeLogName := fmt.Sprintf("%020d.log", activeBase)
	cutoff := time.Now().Add(-ps.cfg.Retention)

	entries, err := os.ReadDir(ps.dir)
	if err != nil {
		ps.logger.Error("log cleaner: failed to read directory",
			"dir", ps.dir,
			"error", err,
		)
		return
	}

	for _, e := range entries {
		name := e.Name()

		// Only consider closed .log segments; skip the active one.
		if !strings.HasSuffix(name, ".log") || name == activeLogName {
			continue
		}

		info, err := e.Info()
		if err != nil {
			ps.logger.Warn("log cleaner: stat failed", "file", name, "error", err)
			continue
		}

		if info.ModTime().After(cutoff) {
			continue // not yet expired
		}

		// Delete the .log file.
		logPath := filepath.Join(ps.dir, name)
		if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
			ps.logger.Error("log cleaner: failed to remove log segment",
				"path", logPath,
				"error", err,
			)
			continue
		}
		ps.logger.Info("log cleaner: removed expired segment", "path", logPath)

		// Delete the paired .index file (same stem, different extension).
		stem := strings.TrimSuffix(name, ".log")
		idxPath := filepath.Join(ps.dir, stem+".index")
		if err := os.Remove(idxPath); err != nil && !os.IsNotExist(err) {
			ps.logger.Error("log cleaner: failed to remove index file",
				"path", idxPath,
				"error", err,
			)
			continue
		}
		ps.logger.Info("log cleaner: removed expired index", "path", idxPath)
	}
}
