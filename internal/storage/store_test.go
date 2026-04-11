package storage

import (
	"errors"
	"testing"
)

func openTestStore(t *testing.T) *PartitionStore {
	t.Helper()
	ps, err := OpenPartitionStore(tempDir(t), SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	t.Cleanup(func() { ps.Close() })
	return ps
}

// TestPartitionStore_Append_ReturnsOffset verifies the first append returns offset 0.
func TestPartitionStore_Append_ReturnsOffset(t *testing.T) {
	ps := openTestStore(t)

	off, err := ps.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off != 0 {
		t.Errorf("first offset = %d, want 0", off)
	}
}

// TestPartitionStore_Append_SequentialOffsets verifies offsets advance correctly.
func TestPartitionStore_Append_SequentialOffsets(t *testing.T) {
	ps := openTestStore(t)

	payloads := [][]byte{[]byte("aaa"), []byte("bb"), []byte("cccc")}
	expected := uint64(0)
	for _, p := range payloads {
		off, err := ps.Append(p)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if off != expected {
			t.Errorf("offset = %d, want %d", off, expected)
		}
		expected += uint64(recordHeaderSize + len(p))
	}
}

// TestPartitionStore_Append_DataPersists verifies appended data is readable via ReadAt.
func TestPartitionStore_Append_DataPersists(t *testing.T) {
	ps := openTestStore(t)

	payload := []byte("record-batch-bytes")
	off, err := ps.Append(payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := ps.active.ReadAt(off)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("ReadAt = %q, want %q", got, payload)
	}
}

// TestPartitionStore_Rollover verifies a new segment is created when full.
func TestPartitionStore_Rollover(t *testing.T) {
	// maxSize = 20 bytes: fits one record of header(4)+payload(16) = 20.
	ps, err := OpenPartitionStore(tempDir(t), SegmentConfig{MaxSize: 20})
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	payload := []byte("exactly16bytes!!")  // 16 bytes → record = 20 bytes
	off1, err := ps.Append(payload)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if off1 != 0 {
		t.Errorf("first offset = %d, want 0", off1)
	}

	// Second append must trigger rollover (segment full).
	off2, err := ps.Append([]byte("x"))
	if err != nil {
		t.Fatalf("second Append (after rollover): %v", err)
	}
	// After rollover the new segment starts at 0 internally.
	if off2 != 0 {
		t.Errorf("post-rollover offset = %d, want 0", off2)
	}
}

// TestPartitionStore_Flush propagates to the active segment without error.
func TestPartitionStore_Flush(t *testing.T) {
	ps := openTestStore(t)
	ps.Append([]byte("data"))

	if err := ps.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
	}
}

// TestPartitionStore_Close flushes and closes without error.
func TestPartitionStore_Close(t *testing.T) {
	ps, err := OpenPartitionStore(tempDir(t), SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	ps.Append([]byte("bye"))

	if err := ps.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestPartitionStore_InvalidDir verifies an error on a bad path.
func TestPartitionStore_InvalidDir(t *testing.T) {
	_, err := OpenPartitionStore("/nonexistent/bad/path", SegmentConfig{})
	if err == nil {
		t.Fatal("expected error for invalid dir, got nil")
	}
}

// TestPartitionStore_ImplementsAppendStore verifies the interface is satisfied.
func TestPartitionStore_ImplementsAppendStore(t *testing.T) {
	ps := openTestStore(t)
	var _ AppendStore = ps // compile-time check
}

// TestPartitionStore_ErrSegmentFull_Propagates verifies that when a single
// record is too large even for a fresh segment, the error is surfaced.
func TestPartitionStore_ErrSegmentFull_Propagates(t *testing.T) {
	// maxSize = 5: too small for any record (header alone is 4 bytes + payload).
	ps, err := OpenPartitionStore(tempDir(t), SegmentConfig{MaxSize: 5})
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	_, err = ps.Append([]byte("toobig"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrSegmentFull) {
		// After rollover the new segment will also be full, so the wrapped error
		// chain must contain ErrSegmentFull.
		t.Errorf("expected ErrSegmentFull in chain, got: %v", err)
	}
}
