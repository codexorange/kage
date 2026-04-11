package storage

import (
	"fmt"
	"sync"
)

// AppendStore is the interface the server layer uses to persist record batches.
// It is satisfied by *PartitionStore and can be faked in tests.
type AppendStore interface {
	// Append writes the raw RecordBatch bytes and returns the byte offset at
	// which the record was written.
	Append(data []byte) (offset uint64, err error)
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
	// nextBase is the logical offset for the next segment, set to the byte
	// size of the current segment when it rolls over.
	nextBase uint64
}

// OpenPartitionStore opens (or creates) a PartitionStore rooted at dir.
// It always starts with base offset 0 on the first segment.
func OpenPartitionStore(dir string, cfg SegmentConfig) (*PartitionStore, error) {
	seg, err := OpenSegment(dir, 0, cfg)
	if err != nil {
		return nil, fmt.Errorf("storage: open partition store: %w", err)
	}
	return &PartitionStore{
		dir:    dir,
		cfg:    cfg,
		active: seg,
	}, nil
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
