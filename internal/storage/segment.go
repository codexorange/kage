package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// DefaultMaxSegmentSize is 1 GiB — the maximum bytes a single segment file
	// may hold before the caller must roll over to a new segment.
	DefaultMaxSegmentSize = 1 << 30 // 1 GiB

	// recordHeaderSize is the number of bytes used to encode the payload length
	// that precedes every record on disk: uint32 big-endian.
	recordHeaderSize = 4

	// writeBufferSize is the capacity of the in-memory bufio.Writer.
	writeBufferSize = 256 << 10 // 256 KiB
)

// ErrSegmentFull is returned by Append when the segment has reached its
// maximum configured size. The caller should open a new segment.
var ErrSegmentFull = errors.New("segment is full")

// ErrInvalidOffset is returned by Read when offset is out of the segment's
// written range.
var ErrInvalidOffset = errors.New("storage: offset out of range")

// ErrInvalidSize is returned by Read when size is ≤ 0 or the requested range
// [offset, offset+size) exceeds the segment's written data.
var ErrInvalidSize = errors.New("storage: invalid read size")

// Segment is an append-only, length-prefixed log file.
//
// On-disk record layout:
//
//	┌────────────────────┬───────────────────┐
//	│ length  (uint32 BE)│ payload  (N bytes)│
//	└────────────────────┴───────────────────┘
//
// The offset returned by Append is the byte position of the length field
// within the file, so readers can seek directly to any previously appended
// record.
//
// Each Segment owns a paired *Index.  A sparse index entry is written
// automatically every IndexIntervalBytes of log data.
//
// Segment is safe for concurrent use.
type Segment struct {
	mu         sync.Mutex
	file       *os.File
	bw         *bufio.Writer
	idx        *Index
	baseOffset uint64 // first logical offset this segment covers
	size       int64  // bytes written to the file (including buffered)
	maxSize    int64
}

// SegmentConfig holds optional parameters for OpenSegment and PartitionStore.
type SegmentConfig struct {
	// MaxSize is the maximum number of bytes the segment file may grow to.
	// Defaults to DefaultMaxSegmentSize when zero.
	MaxSize int64

	// Retention is how long closed segments are kept before the log cleaner
	// deletes them.  Zero (the default) disables retention-based cleanup.
	Retention time.Duration
}

// OpenSegment opens or creates a segment file in dir.
//
//   - baseOffset identifies the segment; the filename encodes it as a
//     zero-padded 20-digit decimal number with a ".log" extension.
//   - If the file already exists its current size is used as the starting
//     write cursor (append-mode resume).
func OpenSegment(dir string, baseOffset uint64, cfg SegmentConfig) (*Segment, error) {
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultMaxSegmentSize
	}

	name := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open segment %q: %w", name, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: stat segment %q: %w", name, err)
	}

	idx, err := openIndex(dir, baseOffset)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Segment{
		file:       f,
		bw:         bufio.NewWriterSize(f, writeBufferSize),
		idx:        idx,
		baseOffset: baseOffset,
		size:       info.Size(),
		maxSize:    cfg.MaxSize,
	}, nil
}

// Append writes payload to the segment and returns the byte offset at which
// the record's length header was written.
//
// Returns ErrSegmentFull (without writing anything) when the segment cannot
// accommodate the record without exceeding maxSize.
func (s *Segment) Append(payload []byte) (offset uint64, err error) {
	recordSize := int64(recordHeaderSize + len(payload))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.size+recordSize > s.maxSize {
		return 0, ErrSegmentFull
	}

	offset = uint64(s.size)

	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))

	if _, err = s.bw.Write(hdr[:]); err != nil {
		return 0, fmt.Errorf("storage: write record header: %w", err)
	}
	if _, err = s.bw.Write(payload); err != nil {
		return 0, fmt.Errorf("storage: write record payload: %w", err)
	}

	s.size += recordSize

	if err = s.idx.maybeAppend(offset, uint32(offset), recordSize); err != nil {
		return 0, fmt.Errorf("storage: update index: %w", err)
	}

	return offset, nil
}

// Flush commits all buffered writes to the OS and then calls fsync on both
// the log file and the index file to guarantee physical durability.
func (s *Segment) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.bw.Flush(); err != nil {
		return fmt.Errorf("storage: flush segment: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("storage: fsync segment: %w", err)
	}
	if err := s.idx.Flush(); err != nil {
		return err
	}
	return nil
}

// Index returns the sparse Index associated with this segment.
// The returned pointer must not be used concurrently with Append.
func (s *Segment) Index() *Index {
	return s.idx
}

// ReadAt reads the record that was written at the given byte offset.
// It flushes buffered data first so recently appended records are visible.
func (s *Segment) ReadAt(offset uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush so the kernel buffer has the latest data.
	if err := s.bw.Flush(); err != nil {
		return nil, fmt.Errorf("storage: flush before read: %w", err)
	}

	var hdr [recordHeaderSize]byte
	if _, err := s.file.ReadAt(hdr[:], int64(offset)); err != nil {
		return nil, fmt.Errorf("storage: read record header at %d: %w", offset, err)
	}

	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return []byte{}, nil
	}

	payload := make([]byte, length)
	if _, err := s.file.ReadAt(payload, int64(offset)+recordHeaderSize); err != nil {
		return nil, fmt.Errorf("storage: read record payload at %d: %w", offset, err)
	}
	return payload, nil
}

// Read returns a zero-copy view of [offset, offset+size) in the .log file as
// an *io.SectionReader.
//
// *io.SectionReader wraps s.file (an *os.File, which implements io.ReaderAt)
// with a fixed [offset, offset+size) window. No bytes are read or copied into
// Go heap memory at this point.
//
// The intended use is to pass the returned reader directly to io.Copy with a
// net.Conn as destination:
//
//	r, _ := seg.Read(offset, size)
//	io.Copy(conn, r)
//
// On Linux (the production target, as defined by the Docker image), Go's
// runtime detects that the underlying source is an *os.File via the
// syscall.Conn interface and substitutes the copy with a sendfile(2) syscall,
// transferring data directly from the page cache to the socket buffer.
//
// The lock is released before returning — reads from the SectionReader do not
// hold Segment.mu, so concurrent Append calls are not blocked.
//
// The caller must not use the returned reader after the Segment is closed.
func (s *Segment) Read(offset uint64, size int32) (io.Reader, error) {
	if size <= 0 {
		return nil, ErrInvalidSize
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if int64(offset) >= s.size {
		return nil, ErrInvalidOffset
	}
	if int64(offset)+int64(size) > s.size {
		return nil, ErrInvalidSize
	}

	// Flush so any buffered-but-not-yet-written bytes are visible to pread(2)
	// calls that the SectionReader will issue via file.ReadAt.
	if err := s.bw.Flush(); err != nil {
		return nil, fmt.Errorf("storage: flush before read: %w", err)
	}

	// io.NewSectionReader builds a stateless window over s.file.
	// s.file is an *os.File → implements io.ReaderAt via pread(2).
	// No allocation beyond the 40-byte SectionReader struct itself.
	return io.NewSectionReader(s.file, int64(offset), int64(size)), nil
}

// Size returns the number of bytes written to the segment (including any
// data still in the write buffer).
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// BaseOffset returns the logical base offset this segment was opened with.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// Close flushes buffered writes and closes both the log and index files.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.bw.Flush(); err != nil {
		return fmt.Errorf("storage: flush on close: %w", err)
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("storage: close segment file: %w", err)
	}
	if err := s.idx.Close(); err != nil {
		return err
	}
	return nil
}
