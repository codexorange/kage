package storage

import (
	"errors"
	"os"
	"testing"
)

// openTestIndex opens an Index in dir with the given baseOffset and registers
// Close on cleanup.
func openTestIndex(t *testing.T, dir string, base uint64) *Index {
	t.Helper()
	idx, err := openIndex(dir, base)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	t.Cleanup(func() { idx.Close() })
	return idx
}

// TestIndex_EmptyLookup verifies ErrOffsetNotFound on an empty index.
func TestIndex_EmptyLookup(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	_, err := idx.Lookup(0)
	if !errors.Is(err, ErrOffsetNotFound) {
		t.Errorf("empty Lookup: want ErrOffsetNotFound, got %v", err)
	}
}

// TestIndex_FileNaming verifies the .index file is created with the expected name.
func TestIndex_FileNaming(t *testing.T) {
	dir := tempDir(t)
	openTestIndex(t, dir, 42)

	expected := dir + "/00000000000000000042.index"
	if _, err := os.Stat(expected); err != nil {
		t.Errorf("index file not found at %q: %v", expected, err)
	}
}

// TestIndex_MaybeAppend_BelowThreshold verifies no entry is written when fewer
// than IndexIntervalBytes have been accumulated.
func TestIndex_MaybeAppend_BelowThreshold(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	// Write IndexIntervalBytes - 1 bytes worth of record data.
	if err := idx.maybeAppend(0, 0, IndexIntervalBytes-1); err != nil {
		t.Fatalf("maybeAppend: %v", err)
	}
	if idx.Len() != 0 {
		t.Errorf("Len = %d, want 0 (threshold not yet reached)", idx.Len())
	}
}

// TestIndex_MaybeAppend_AtThreshold verifies an entry is written exactly at
// IndexIntervalBytes.
func TestIndex_MaybeAppend_AtThreshold(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	if err := idx.maybeAppend(100, 200, IndexIntervalBytes); err != nil {
		t.Fatalf("maybeAppend: %v", err)
	}
	if idx.Len() != 1 {
		t.Fatalf("Len = %d, want 1", idx.Len())
	}
	if idx.entries[0].offset != 100 {
		t.Errorf("entry.offset = %d, want 100", idx.entries[0].offset)
	}
	if idx.entries[0].position != 200 {
		t.Errorf("entry.position = %d, want 200", idx.entries[0].position)
	}
}

// TestIndex_MaybeAppend_AccumulatesAcrossCalls verifies bytes accumulate
// across multiple calls before the threshold is reached.
func TestIndex_MaybeAppend_AccumulatesAcrossCalls(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	half := int64(IndexIntervalBytes / 2)
	// First half — no entry yet.
	if err := idx.maybeAppend(10, 10, half); err != nil {
		t.Fatalf("first maybeAppend: %v", err)
	}
	if idx.Len() != 0 {
		t.Errorf("after first half: Len = %d, want 0", idx.Len())
	}

	// Second half — threshold crossed, entry for the second call's offset/pos.
	if err := idx.maybeAppend(20, 20, half); err != nil {
		t.Fatalf("second maybeAppend: %v", err)
	}
	if idx.Len() != 1 {
		t.Fatalf("after second half: Len = %d, want 1", idx.Len())
	}
	if idx.entries[0].offset != 20 {
		t.Errorf("entry.offset = %d, want 20", idx.entries[0].offset)
	}
}

// TestIndex_MaybeAppend_CounterResets verifies bytesWrittenSinceLastEntry
// resets to zero after an entry is written so the next window starts fresh.
func TestIndex_MaybeAppend_CounterResets(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	// Trigger first entry.
	if err := idx.maybeAppend(0, 0, IndexIntervalBytes); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Write less than threshold again — no new entry.
	if err := idx.maybeAppend(1, 1, IndexIntervalBytes-1); err != nil {
		t.Fatalf("second: %v", err)
	}
	if idx.Len() != 1 {
		t.Errorf("Len = %d, want 1 (counter should have reset)", idx.Len())
	}
	// Tip over the threshold.
	if err := idx.maybeAppend(2, 2, 1); err != nil {
		t.Fatalf("third: %v", err)
	}
	if idx.Len() != 2 {
		t.Errorf("Len = %d, want 2", idx.Len())
	}
}

// TestIndex_Lookup_ExactMatch verifies Lookup returns the position when the
// target offset exactly matches an indexed entry.
func TestIndex_Lookup_ExactMatch(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	idx.maybeAppend(1000, 512, IndexIntervalBytes)

	pos, err := idx.Lookup(1000)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if pos != 512 {
		t.Errorf("position = %d, want 512", pos)
	}
}

// TestIndex_Lookup_BetweenEntries verifies Lookup returns the floor entry
// when the target falls between two indexed offsets.
func TestIndex_Lookup_BetweenEntries(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	// Entry 1: offset=100, position=0
	idx.maybeAppend(100, 0, IndexIntervalBytes)
	// Entry 2: offset=200, position=4096
	idx.maybeAppend(200, 4096, IndexIntervalBytes)

	// Target 150 is between 100 and 200 → should return position of offset=100.
	pos, err := idx.Lookup(150)
	if err != nil {
		t.Fatalf("Lookup(150): %v", err)
	}
	if pos != 0 {
		t.Errorf("position = %d, want 0", pos)
	}
}

// TestIndex_Lookup_BeyondLastEntry verifies Lookup returns the last entry
// when the target exceeds all indexed offsets.
func TestIndex_Lookup_BeyondLastEntry(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	idx.maybeAppend(100, 0, IndexIntervalBytes)
	idx.maybeAppend(200, 4096, IndexIntervalBytes)

	pos, err := idx.Lookup(99999)
	if err != nil {
		t.Fatalf("Lookup(99999): %v", err)
	}
	if pos != 4096 {
		t.Errorf("position = %d, want 4096", pos)
	}
}

// TestIndex_Lookup_BeforeFirstEntry verifies ErrOffsetNotFound when the
// target is smaller than every indexed offset.
func TestIndex_Lookup_BeforeFirstEntry(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	idx.maybeAppend(500, 1024, IndexIntervalBytes)

	_, err := idx.Lookup(499)
	if !errors.Is(err, ErrOffsetNotFound) {
		t.Errorf("want ErrOffsetNotFound, got %v", err)
	}
}

// TestIndex_Lookup_MultipleEntries exercises binary search over many entries.
func TestIndex_Lookup_MultipleEntries(t *testing.T) {
	idx := openTestIndex(t, tempDir(t), 0)

	// Insert 10 entries: offsets 0, 100, 200, …, 900; positions 0, 4096, …
	for i := 0; i < 10; i++ {
		idx.maybeAppend(uint64(i*100), uint32(i*IndexIntervalBytes), IndexIntervalBytes)
	}

	cases := []struct {
		target  uint64
		wantPos uint32
	}{
		{0, 0},
		{50, 0}, // between 0 and 100 → floor is 0
		{100, 4096},
		{199, 4096}, // between 100 and 200 → floor is 100
		{900, 9 * IndexIntervalBytes},
		{1000, 9 * IndexIntervalBytes}, // beyond last
	}

	for _, c := range cases {
		pos, err := idx.Lookup(c.target)
		if err != nil {
			t.Errorf("Lookup(%d): unexpected error %v", c.target, err)
			continue
		}
		if pos != c.wantPos {
			t.Errorf("Lookup(%d) = %d, want %d", c.target, pos, c.wantPos)
		}
	}
}

// TestIndex_Persist_ReloadEntries verifies that entries written to disk are
// correctly reloaded when the index is reopened.
func TestIndex_Persist_ReloadEntries(t *testing.T) {
	dir := tempDir(t)

	// Session 1: write two entries.
	idx1, err := openIndex(dir, 0)
	if err != nil {
		t.Fatalf("openIndex (first): %v", err)
	}
	idx1.maybeAppend(100, 0, IndexIntervalBytes)
	idx1.maybeAppend(200, 4096, IndexIntervalBytes)
	if err := idx1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Session 2: reopen and verify entries survive.
	idx2, err := openIndex(dir, 0)
	if err != nil {
		t.Fatalf("openIndex (second): %v", err)
	}
	defer idx2.Close()

	if idx2.Len() != 2 {
		t.Fatalf("reloaded Len = %d, want 2", idx2.Len())
	}

	pos, err := idx2.Lookup(100)
	if err != nil {
		t.Fatalf("Lookup after reload: %v", err)
	}
	if pos != 0 {
		t.Errorf("reloaded position for offset 100 = %d, want 0", pos)
	}

	pos, err = idx2.Lookup(200)
	if err != nil {
		t.Fatalf("Lookup after reload (200): %v", err)
	}
	if pos != 4096 {
		t.Errorf("reloaded position for offset 200 = %d, want 4096", pos)
	}
}

// TestIndex_Persist_BytesWrittenCounterResets verifies that reopening resets
// bytesWrittenSinceLastEntry to 0 (fresh window).
func TestIndex_Persist_BytesWrittenCounterResets(t *testing.T) {
	dir := tempDir(t)

	idx1, err := openIndex(dir, 0)
	if err != nil {
		t.Fatalf("openIndex (first): %v", err)
	}
	// Write just below the threshold, then close.
	idx1.maybeAppend(0, 0, IndexIntervalBytes-1)
	if err := idx1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the counter should start at 0, not carry over the partial window.
	idx2, err := openIndex(dir, 0)
	if err != nil {
		t.Fatalf("openIndex (second): %v", err)
	}
	defer idx2.Close()

	if idx2.bytesWrittenSinceLastEntry != 0 {
		t.Errorf("bytesWrittenSinceLastEntry after reopen = %d, want 0",
			idx2.bytesWrittenSinceLastEntry)
	}
}

// TestSegment_IndexIntegration verifies that appending enough data to the
// segment automatically produces index entries readable via Lookup.
func TestSegment_IndexIntegration(t *testing.T) {
	dir := tempDir(t)
	seg, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	// Write records until we cross IndexIntervalBytes twice.
	// Each record: 4-byte header + 100-byte payload = 104 bytes.
	// Threshold = 4096 bytes → need ≥ 40 records per interval.
	payload := make([]byte, 100)

	var firstIndexedOffset uint64
	firstEntry := true

	for i := 0; i < 100; i++ {
		off, err := seg.Append(payload)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		if firstEntry && seg.idx.Len() > 0 {
			firstIndexedOffset = off
			firstEntry = false
		}
	}

	if seg.idx.Len() < 2 {
		t.Fatalf("expected ≥2 index entries after 100 records, got %d", seg.idx.Len())
	}

	// Lookup must find a position ≤ firstIndexedOffset.
	pos, err := seg.idx.Lookup(firstIndexedOffset)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if uint64(pos) > firstIndexedOffset {
		t.Errorf("Lookup position %d > firstIndexedOffset %d", pos, firstIndexedOffset)
	}
}

// TestSegment_Index_Accessor verifies Segment.Index() returns the live index.
func TestSegment_Index_Accessor(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	if seg.Index() == nil {
		t.Fatal("Index() returned nil")
	}
	if seg.Index() != seg.idx {
		t.Fatal("Index() does not return the segment's own index")
	}
}

// TestSegment_IndexPersistsAcrossReopen verifies the index file survives a
// Segment close+reopen cycle.
func TestSegment_IndexPersistsAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	payload := make([]byte, 100)

	// First session: write enough records to generate at least one index entry.
	seg1, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment (first): %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := seg1.Append(payload); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	entriesAfterFirst := seg1.idx.Len()
	if err := seg1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second session: index must reload the same entries.
	seg2, err := OpenSegment(dir, 0, SegmentConfig{})
	if err != nil {
		t.Fatalf("OpenSegment (second): %v", err)
	}
	defer seg2.Close()

	if seg2.idx.Len() != entriesAfterFirst {
		t.Errorf("index entries after reopen = %d, want %d",
			seg2.idx.Len(), entriesAfterFirst)
	}
}
