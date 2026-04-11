package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

// tempDir creates a temporary directory and registers cleanup.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "kage-segment-*")
	if err != nil {
		t.Fatalf("tempDir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func openDefault(t *testing.T, dir string, base uint64) *Segment {
	t.Helper()
	seg, err := OpenSegment(dir, base, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	t.Cleanup(func() { seg.Close() })
	return seg
}

// TestOpenSegment_CreatesFile verifies the segment file is created on disk.
func TestOpenSegment_CreatesFile(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	if seg.BaseOffset() != 0 {
		t.Errorf("BaseOffset = %d, want 0", seg.BaseOffset())
	}
	if seg.Size() != 0 {
		t.Errorf("initial Size = %d, want 0", seg.Size())
	}

	expectedName := fmt.Sprintf("%s/%020d.log", dir, 0)
	if _, err := os.Stat(expectedName); err != nil {
		t.Errorf("segment file not found: %v", err)
	}
}

// TestAppend_ReturnsZeroOffsetForFirst verifies the first record starts at byte 0.
func TestAppend_ReturnsZeroOffsetForFirst(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	offset, err := seg.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset = %d, want 0", offset)
	}
}

// TestAppend_OffsetAdvancesByRecordSize verifies consecutive offsets.
func TestAppend_OffsetAdvancesByRecordSize(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	payloads := [][]byte{
		[]byte("hello"),        // 5 bytes → record = 9
		[]byte("world!!"),      // 7 bytes → record = 11
		[]byte("kage-events"),  // 11 bytes → record = 15
	}

	expected := uint64(0)
	for _, p := range payloads {
		off, err := seg.Append(p)
		if err != nil {
			t.Fatalf("Append(%q): %v", p, err)
		}
		if off != expected {
			t.Errorf("offset = %d, want %d", off, expected)
		}
		expected += uint64(recordHeaderSize + len(p))
	}
}

// TestAppend_ReadAt verifies payload round-trips correctly through disk.
func TestAppend_ReadAt(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	cases := []string{"hello", "world", "kage", "", "binary\x00data"}

	offsets := make([]uint64, len(cases))
	for i, c := range cases {
		off, err := seg.Append([]byte(c))
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		offsets[i] = off
	}

	for i, c := range cases {
		got, err := seg.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("ReadAt[%d] offset=%d: %v", i, offsets[i], err)
		}
		if string(got) != c {
			t.Errorf("ReadAt[%d] = %q, want %q", i, got, c)
		}
	}
}

// TestAppend_SizeTracking verifies Size() stays consistent.
func TestAppend_SizeTracking(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	payload := []byte("hello world")
	_, err := seg.Append(payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := int64(recordHeaderSize + len(payload))
	if got := seg.Size(); got != want {
		t.Errorf("Size = %d, want %d", got, want)
	}
}

// TestErrSegmentFull is returned when a record would exceed maxSize.
func TestErrSegmentFull(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{MaxSize: 10})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	// 11-byte payload + 4-byte header = 15 > maxSize(10).
	_, err = seg.Append([]byte("12345678901"))
	if !errors.Is(err, ErrSegmentFull) {
		t.Errorf("expected ErrSegmentFull, got %v", err)
	}
	// Nothing should have been written.
	if seg.Size() != 0 {
		t.Errorf("size after rejected append = %d, want 0", seg.Size())
	}
}

// TestErrSegmentFull_AfterPartialFill verifies the cap is enforced after some
// records have already been written.
func TestErrSegmentFull_AfterPartialFill(t *testing.T) {
	dir := tempDir(t)
	// maxSize = 9 bytes → fits exactly one record: header(4) + payload(5)
	seg, err := OpenSegment(dir, 0, SegmentConfig{MaxSize: 9})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	if _, err := seg.Append([]byte("hello")); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	_, err = seg.Append([]byte("x"))
	if !errors.Is(err, ErrSegmentFull) {
		t.Errorf("expected ErrSegmentFull on second append, got %v", err)
	}
}

// TestSegment_DefaultMaxSize verifies zero MaxSize defaults to 1 GiB.
func TestSegment_DefaultMaxSize(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	if seg.maxSize != DefaultMaxSegmentSize {
		t.Errorf("maxSize = %d, want %d", seg.maxSize, DefaultMaxSegmentSize)
	}
}

// TestSegment_ResumeFromExistingFile verifies that reopening a segment picks
// up the correct size and that new appends land at the right offsets.
func TestSegment_ResumeFromExistingFile(t *testing.T) {
	dir := tempDir(t)

	// First session: write one record.
	seg1, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment (first): %v", err)
	}
	payload := []byte("persistent")
	off1, err := seg1.Append(payload)
	if err != nil {
		t.Fatalf("Append (first): %v", err)
	}
	if err := seg1.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	expectedSize := int64(recordHeaderSize + len(payload))

	// Second session: reopen and verify size is restored.
	seg2, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment (second): %v", err)
	}
	defer seg2.Close()

	if seg2.Size() != expectedSize {
		t.Errorf("resumed Size = %d, want %d", seg2.Size(), expectedSize)
	}

	// New append should start right after the first record.
	off2, err := seg2.Append([]byte("new"))
	if err != nil {
		t.Fatalf("Append (second): %v", err)
	}
	if off2 != uint64(expectedSize) {
		t.Errorf("second offset = %d, want %d", off2, expectedSize)
	}

	// Both records must be readable.
	got1, err := seg2.ReadAt(off1)
	if err != nil {
		t.Fatalf("ReadAt first: %v", err)
	}
	if string(got1) != string(payload) {
		t.Errorf("first record = %q, want %q", got1, payload)
	}

	got2, err := seg2.ReadAt(off2)
	if err != nil {
		t.Fatalf("ReadAt second: %v", err)
	}
	if string(got2) != "new" {
		t.Errorf("second record = %q, want %q", got2, "new")
	}
}

// TestSegment_BaseOffset verifies non-zero base offsets are stored correctly.
func TestSegment_BaseOffset(t *testing.T) {
	dir := tempDir(t)
	const base = uint64(1_000_000)
	seg, err := OpenSegment(dir, base, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	if seg.BaseOffset() != base {
		t.Errorf("BaseOffset = %d, want %d", seg.BaseOffset(), base)
	}
}

// TestSegment_ConcurrentAppend verifies that concurrent Append calls produce
// non-overlapping, readable records.
func TestSegment_ConcurrentAppend(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	const goroutines = 50
	const recordsEach = 20

	type result struct {
		offset  uint64
		payload []byte
	}

	results := make(chan result, goroutines*recordsEach)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := 0; r < recordsEach; r++ {
				p := []byte(fmt.Sprintf("goroutine-%d-record-%d", id, r))
				off, err := seg.Append(p)
				if err != nil {
					t.Errorf("concurrent Append: %v", err)
					return
				}
				results <- result{offset: off, payload: p}
			}
		}(g)
	}

	wg.Wait()
	close(results)

	// Verify every record can be read back correctly.
	for res := range results {
		got, err := seg.ReadAt(res.offset)
		if err != nil {
			t.Errorf("ReadAt %d: %v", res.offset, err)
			continue
		}
		if string(got) != string(res.payload) {
			t.Errorf("ReadAt %d = %q, want %q", res.offset, got, res.payload)
		}
	}
}

// TestFlush verifies that Flush makes buffered data readable.
func TestFlush(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	payload := []byte("flush-me")
	off, err := seg.Append(payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read directly from the file (bypassing bufio) to confirm data is there.
	hdr := make([]byte, recordHeaderSize)
	if _, err := seg.file.ReadAt(hdr, int64(off)); err != nil {
		t.Fatalf("raw ReadAt header: %v", err)
	}
	body := make([]byte, len(payload))
	if _, err := seg.file.ReadAt(body, int64(off)+recordHeaderSize); err != nil {
		t.Fatalf("raw ReadAt body: %v", err)
	}
	if string(body) != string(payload) {
		t.Errorf("after Flush body = %q, want %q", body, payload)
	}
}

// TestClose_IdempotentFlush verifies that Close flushes buffered data.
func TestClose_IdempotentFlush(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	payload := []byte("close-flush")
	if _, err := seg.Append(payload); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify the data persisted.
	seg2, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment (reopen): %v", err)
	}
	defer seg2.Close()

	got, err := seg2.ReadAt(0)
	if err != nil {
		t.Fatalf("ReadAt after reopen: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("after reopen = %q, want %q", got, payload)
	}
}

// TestOpenSegment_InvalidDir verifies an error is returned for a bad path.
func TestOpenSegment_InvalidDir(t *testing.T) {
	_, err := OpenSegment("/nonexistent/path/that/does/not/exist", 0, SegmentConfig{})
	if err == nil {
		t.Fatal("expected error for invalid dir, got nil")
	}
}

// TestFlush_AfterClose verifies Flush returns an error when the underlying
// file has already been closed.
func TestFlush_AfterClose(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 42, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	// Close the raw file directly so bufio.Flush hits a write error.
	seg.file.Close()

	// Manually dirty the bufio buffer so Flush actually tries to write.
	seg.bw.WriteByte(0xFF)

	if err := seg.Flush(); err == nil {
		t.Fatal("expected Flush to fail on closed file, got nil")
	}
}

// TestReadAt_InvalidOffset verifies an error is returned for an out-of-range offset.
func TestReadAt_InvalidOffset(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	_, err := seg.ReadAt(9999)
	if err == nil {
		t.Fatal("expected error reading at invalid offset, got nil")
	}
}

// TestAppend_EmptyPayload verifies zero-length records are handled correctly.
func TestAppend_EmptyPayload(t *testing.T) {
	dir := tempDir(t)
	seg := openDefault(t, dir, 0)

	off, err := seg.Append([]byte{})
	if err != nil {
		t.Fatalf("Append empty: %v", err)
	}

	got, err := seg.ReadAt(off)
	if err != nil {
		t.Fatalf("ReadAt empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty record = %q, want empty", got)
	}
}
