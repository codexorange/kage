package storage

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

// ── Index error paths ─────────────────────────────────────────────────────────

// TestIndex_Close_FlushError verifies that Index.Close returns an error when
// the underlying file is already closed (so the bufio.Writer flush fails).
func TestIndex_Close_FlushError(t *testing.T) {
	idx, err := openIndex(tempDir(t), 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	// Close the raw file so the bufio.Writer cannot flush.
	idx.file.Close()
	// Dirty the buffer so Flush actually attempts a write.
	idx.bw.WriteByte(0xFF)

	if err := idx.Close(); err == nil {
		t.Fatal("expected error from Close with closed file, got nil")
	}
}

// TestIndex_Close_FileCloseError verifies that Index.Close surfaces a file
// close error when the flush succeeds but closing the file descriptor fails.
func TestIndex_Close_FileCloseError(t *testing.T) {
	idx, err := openIndex(tempDir(t), 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	// Close the fd first so the second Close inside idx.Close() returns an error.
	idx.file.Close()
	// bw buffer is clean (no pending write), so Flush() is a no-op write-wise.
	// The subsequent idx.file.Close() will return "file already closed".
	if err := idx.Close(); err == nil {
		t.Fatal("expected error from double-close of index file, got nil")
	}
}

// TestIndex_Flush_Error verifies that Index.Flush propagates write errors.
func TestIndex_Flush_Error(t *testing.T) {
	idx, err := openIndex(tempDir(t), 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	defer idx.file.Close()

	// Close the underlying file, then dirty the buffer.
	idx.file.Close()
	idx.bw.WriteByte(0xFF)

	if err := idx.Flush(); err == nil {
		t.Fatal("expected Flush error on closed file, got nil")
	}
}

// TestOpenIndex_LoadEntriesError verifies that openIndex surfaces a loadEntries
// error when the file cannot be stat'd.  We simulate this by replacing the
// index file with a directory of the same name so os.OpenFile itself fails.
func TestOpenIndex_LoadEntriesError(t *testing.T) {
	dir := tempDir(t)
	// Create a directory where the index file would be, so OpenFile fails.
	indexName := dir + "/00000000000000000000.index"
	if err := os.Mkdir(indexName, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	_, err := openIndex(dir, 0)
	if err == nil {
		t.Fatal("expected error when index path is a directory, got nil")
	}
}

// ── Segment error paths ───────────────────────────────────────────────────────

// TestOpenSegment_IndexOpenError verifies that OpenSegment fails gracefully
// when the paired index file cannot be opened (directory in its place).
func TestOpenSegment_IndexOpenError(t *testing.T) {
	dir := tempDir(t)
	// Pre-create the index path as a directory so openIndex fails.
	indexName := dir + "/00000000000000000000.index"
	if err := os.Mkdir(indexName, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	_, err := OpenSegment(dir, 0, SegmentConfig{})
	if err == nil {
		t.Fatal("expected OpenSegment to fail when index cannot be opened, got nil")
	}
}

// TestSegment_Append_WriteHeaderError verifies Append returns an error when
// the underlying write fails.  We fill the bufio.Writer to capacity then close
// the file, so the next Append must flush and will hit the closed-file error.
func TestSegment_Append_WriteHeaderError(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.idx.Close()

	// Replace bw with a 1-byte buffer so the very next Write will flush.
	seg.bw = bufio.NewWriterSize(seg.file, 1)

	// Close the underlying file so any flush will fail.
	seg.file.Close()

	_, err = seg.Append([]byte("data"))
	if err == nil {
		t.Fatal("expected Append error on closed log file, got nil")
	}
}

// TestSegment_Close_FileCloseError verifies that Segment.Close surfaces an
// error when the log file fd is already closed.
func TestSegment_Close_FileCloseError(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	// Close the raw file descriptor so seg.file.Close() returns an error,
	// but the bufio buffer is empty so Flush() is a no-op.
	seg.file.Close()

	if err := seg.Close(); err == nil {
		t.Fatal("expected Close error on already-closed file, got nil")
	}
}

// TestSegment_Flush_IndexError verifies that Segment.Flush propagates an index
// flush error.
func TestSegment_Flush_IndexError(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.file.Close()

	// Break the index's underlying file and dirty its buffer.
	seg.idx.file.Close()
	seg.idx.bw.WriteByte(0xFF)

	if err := seg.Flush(); err == nil {
		t.Fatal("expected Flush to fail when index file is closed, got nil")
	}
}

// ── PartitionStore / rollover error paths ─────────────────────────────────────

// TestPartitionStore_Rollover_OpenError verifies that rollover returns an error
// when the new segment cannot be opened (directory in segment file's place).
func TestPartitionStore_Rollover_OpenError(t *testing.T) {
	dir := tempDir(t)
	// maxSize = 9 so one 5-byte payload fills it (4+5 = 9).
	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{MaxSize: 9}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	// Fill the first segment.
	if _, err := ps.Append([]byte("hello")); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Place a directory where the next segment file would land so OpenSegment
	// fails during rollover.  The next segment starts at offset = 9.
	nextSegName := dir + "/00000000000000000009.log"
	if err := os.Mkdir(nextSegName, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	_, err = ps.Append([]byte("x"))
	if err == nil {
		t.Fatal("expected rollover to fail when new segment path is a directory, got nil")
	}
}

// TestPartitionStore_Rollover_CloseError verifies that rollover surfaces a
// close error on the current segment.
func TestPartitionStore_Rollover_CloseError(t *testing.T) {
	dir := tempDir(t)
	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{MaxSize: 9}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}

	// Fill the segment.
	if _, err := ps.Append([]byte("hello")); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Force the active segment's file close to fail by closing it early
	// and dirtying the buffer so Flush fails.
	ps.active.file.Close()
	ps.active.bw.WriteByte(0xFF)

	_, err = ps.Append([]byte("x"))
	if err == nil {
		t.Fatal("expected error when segment close fails during rollover, got nil")
	}
	// Also close the index to avoid leaking the fd.
	ps.active.idx.Close()
}

// TestPartitionStore_Append_ErrAfterRollover verifies that an error from
// Append on the fresh segment (after a successful rollover) is surfaced.
func TestPartitionStore_Append_ErrAfterRollover(t *testing.T) {
	dir := tempDir(t)
	// maxSize = 9: one 5-byte payload fills it exactly.
	// Use maxSize = 5 for new segment: too small for any record.
	// We can't change cfg mid-flight, but we can test the double-ErrSegmentFull
	// path in Append: if both the current and the fresh segment return full,
	// the error is wrapped and returned.
	// Here we use maxSize = 4 (header only, no payload fits).
	ps, err := OpenPartitionStore(context.Background(), dir, SegmentConfig{MaxSize: 4}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("OpenPartitionStore: %v", err)
	}
	defer ps.Close()

	_, err = ps.Append([]byte("x"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrSegmentFull) {
		t.Errorf("expected ErrSegmentFull in chain, got: %v", err)
	}
}
