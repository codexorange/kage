package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// IndexIntervalBytes is the number of log bytes that must be written before
	// a new sparse index entry is recorded.  4 KiB gives ≤ 262 144 entries for
	// a 1 GiB segment (~3 MiB of index RAM).
	IndexIntervalBytes = 4096

	// indexEntrySize is the on-disk size of one entry:
	//   offset   uint64 BE  (8 bytes)
	//   position uint32 BE  (4 bytes)
	indexEntrySize = 12

	// indexWriteBufferSize is the capacity of the index bufio.Writer.
	indexWriteBufferSize = 64 << 10 // 64 KiB
)

// ErrOffsetNotFound is returned by Lookup when no index entry covers the
// requested offset.
var ErrOffsetNotFound = fmt.Errorf("storage: offset not found in index")

// entry is the in-memory representation of one index record.
type entry struct {
	offset   uint64 // logical message offset (absolute)
	position uint32 // byte position in the .log file
}

// Index is a sparse, append-only index file that maps logical offsets to byte
// positions inside the associated segment's .log file.
//
// On-disk format — each record is exactly 12 bytes:
//
//	┌──────────────────────┬──────────────────┐
//	│ offset  (uint64 BE)  │ position (uint32)│
//	└──────────────────────┴──────────────────┘
//
// An entry is written for every IndexIntervalBytes of log data appended.
// Lookup performs a binary search and returns the position of the largest
// indexed offset ≤ the target, from which the caller scans the log forward.
//
// Index is NOT safe for concurrent use on its own; it is always called from
// within Segment's mutex.
type Index struct {
	file    *os.File
	bw      *bufio.Writer
	entries []entry // loaded eagerly at open; appended in-memory on write

	// bytesWrittenSinceLastEntry counts log bytes written since the last index
	// entry was recorded.  Resets to 0 each time an entry is flushed.
	bytesWrittenSinceLastEntry int64
}

// openIndex opens or creates the .index file paired with the given .log base
// offset.  Existing entries are loaded into memory so Lookup is O(log n)
// immediately after open.
func openIndex(dir string, baseOffset uint64) (*Index, error) {
	name := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open index %q: %w", name, err)
	}

	entries, err := loadEntries(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: load index %q: %w", name, err)
	}

	return &Index{
		file:    f,
		bw:      bufio.NewWriterSize(f, indexWriteBufferSize),
		entries: entries,
	}, nil
}

// loadEntries reads all existing entries from f into memory.
// f is positioned at the start; on return its position is at EOF.
func loadEntries(f *os.File) ([]entry, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	count := info.Size() / indexEntrySize
	entries := make([]entry, 0, count)

	var buf [indexEntrySize]byte
	for {
		_, err := io.ReadFull(f, buf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry{
			offset:   binary.BigEndian.Uint64(buf[0:8]),
			position: binary.BigEndian.Uint32(buf[8:12]),
		})
	}
	return entries, nil
}

// maybeAppend records a new index entry if enough log bytes have accumulated
// since the last entry.
//
//   - logOffset is the absolute logical offset of the record just appended.
//   - position is the byte position of that record inside the .log file.
//   - recordSize is the total number of bytes the record occupies on disk
//     (header + payload).
//
// Must be called under Segment.mu.
func (idx *Index) maybeAppend(logOffset uint64, position uint32, recordSize int64) error {
	idx.bytesWrittenSinceLastEntry += recordSize

	if idx.bytesWrittenSinceLastEntry < IndexIntervalBytes {
		return nil
	}

	e := entry{offset: logOffset, position: position}

	var buf [indexEntrySize]byte
	binary.BigEndian.PutUint64(buf[0:8], e.offset)
	binary.BigEndian.PutUint32(buf[8:12], e.position)

	if _, err := idx.bw.Write(buf[:]); err != nil {
		return fmt.Errorf("storage: write index entry: %w", err)
	}

	idx.entries = append(idx.entries, e)
	idx.bytesWrittenSinceLastEntry = 0
	return nil
}

// Lookup returns the byte position of the largest indexed offset that is
// less than or equal to target.
//
// If the index is empty or every entry has an offset greater than target,
// ErrOffsetNotFound is returned.
//
// The caller should seek the .log file to the returned position and scan
// forward until it reaches the exact record it wants.
func (idx *Index) Lookup(target uint64) (uint32, error) {
	if len(idx.entries) == 0 {
		return 0, ErrOffsetNotFound
	}

	// Binary search for the rightmost entry whose offset ≤ target.
	lo, hi := 0, len(idx.entries)-1
	result := -1

	for lo <= hi {
		mid := (lo + hi) / 2
		if idx.entries[mid].offset <= target {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if result == -1 {
		return 0, ErrOffsetNotFound
	}
	return idx.entries[result].position, nil
}

// Len returns the number of entries currently held in the in-memory index.
func (idx *Index) Len() int {
	return len(idx.entries)
}

// Flush commits buffered index writes to the OS and then calls fsync to
// guarantee physical durability.
func (idx *Index) Flush() error {
	if err := idx.bw.Flush(); err != nil {
		return fmt.Errorf("storage: flush index: %w", err)
	}
	if err := idx.file.Sync(); err != nil {
		return fmt.Errorf("storage: fsync index: %w", err)
	}
	return nil
}

// Close flushes and closes the index file.
func (idx *Index) Close() error {
	if err := idx.bw.Flush(); err != nil {
		return fmt.Errorf("storage: flush index on close: %w", err)
	}
	if err := idx.file.Close(); err != nil {
		return fmt.Errorf("storage: close index file: %w", err)
	}
	return nil
}
