package storage

import (
	"errors"
	"io"
	"testing"
)

// TestPartitionStore_Read_ReturnsData verifies a round-trip: Append then Read.
func TestPartitionStore_Read_ReturnsData(t *testing.T) {
	ps := openTestStore(t)

	payload := []byte("hello-fetch")
	off, err := ps.Append(payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// The stored record is: 4-byte header + payload.
	recordSize := int32(recordHeaderSize + len(payload))
	r, n, err := ps.Read(off, recordSize)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != recordSize {
		t.Errorf("n = %d, want %d", n, recordSize)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != int(recordSize) {
		t.Errorf("read %d bytes, want %d", len(got), recordSize)
	}
}

// TestPartitionStore_Read_MaxBytesCap verifies the read is capped to maxBytes.
func TestPartitionStore_Read_MaxBytesCap(t *testing.T) {
	ps := openTestStore(t)

	payload := []byte("0123456789") // 10 bytes; record = 14 bytes
	if _, err := ps.Append(payload); err != nil {
		t.Fatalf("Append: %v", err)
	}

	r, n, err := ps.Read(0, 4) // ask for only 4 bytes
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 4 {
		t.Errorf("n = %d, want 4", n)
	}
	got, _ := io.ReadAll(r)
	if len(got) != 4 {
		t.Errorf("got %d bytes, want 4", len(got))
	}
}

// TestPartitionStore_Read_ErrInvalidOffset verifies ErrInvalidOffset when
// the requested offset is beyond the written data.
func TestPartitionStore_Read_ErrInvalidOffset(t *testing.T) {
	ps := openTestStore(t)

	_, _, err := ps.Read(9999, 128)
	if !errors.Is(err, ErrInvalidOffset) {
		t.Errorf("want ErrInvalidOffset, got %v", err)
	}
}

// TestPartitionStore_Read_AvailableCap verifies that when maxBytes exceeds
// the available data, the reader is capped to what is available.
func TestPartitionStore_Read_AvailableCap(t *testing.T) {
	ps := openTestStore(t)

	payload := []byte("short") // record = 9 bytes
	if _, err := ps.Append(payload); err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := int32(recordHeaderSize + len(payload))
	r, n, err := ps.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != want {
		t.Errorf("n = %d, want %d", n, want)
	}
	got, _ := io.ReadAll(r)
	if len(got) != int(want) {
		t.Errorf("got %d bytes, want %d", len(got), want)
	}
}

// TestPartitionStore_Size_EmptyStore verifies Size returns 0 for a fresh store.
func TestPartitionStore_Size_EmptyStore(t *testing.T) {
	ps := openTestStore(t)
	if sz := ps.Size(); sz != 0 {
		t.Errorf("Size = %d, want 0", sz)
	}
}

// TestPartitionStore_Size_AfterAppend verifies Size advances after each Append.
func TestPartitionStore_Size_AfterAppend(t *testing.T) {
	ps := openTestStore(t)

	payload := []byte("data")
	if _, err := ps.Append(payload); err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := int64(recordHeaderSize + len(payload))
	if sz := ps.Size(); sz != want {
		t.Errorf("Size = %d, want %d", sz, want)
	}
}

// TestPartitionStore_ImplementsStore verifies *PartitionStore satisfies Store.
func TestPartitionStore_ImplementsStore(t *testing.T) {
	ps := openTestStore(t)
	var _ Store = ps // compile-time check
}
