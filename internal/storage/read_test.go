package storage

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
)

// appendOne is a helper that appends payload and fatals on error.
func appendOne(t *testing.T, s *Segment, payload []byte) uint64 {
	t.Helper()
	off, err := s.Append(payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return off
}

// readAll drains r into a []byte via io.Copy (no direct slice read).
func readAll(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	return buf.Bytes()
}

// TestRead_ReturnsSectionReader verifies the concrete type is *io.SectionReader.
// This guarantees the caller can cast to access ReadAt and Seek without a copy.
func TestRead_ReturnsSectionReader(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	off := appendOne(t, seg, []byte("hello"))

	r, err := seg.Read(off, int32(recordHeaderSize+5))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, ok := r.(*io.SectionReader); !ok {
		t.Errorf("Read() returned %T, want *io.SectionReader", r)
	}
}

// TestRead_ImplementsRequiredInterfaces verifies *io.SectionReader satisfies
// io.ReaderAt and io.Seeker — needed for range reads and size queries.
func TestRead_ImplementsRequiredInterfaces(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	off := appendOne(t, seg, []byte("ifaces"))

	r, err := seg.Read(off, int32(recordHeaderSize+6))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	sr := r.(*io.SectionReader)

	if _, ok := any(sr).(io.ReaderAt); !ok {
		t.Error("*io.SectionReader must implement io.ReaderAt")
	}
	if _, ok := any(sr).(io.Seeker); !ok {
		t.Error("*io.SectionReader must implement io.Seeker")
	}
}

// TestRead_FullRecord verifies the complete on-disk record (header + payload)
// is delivered correctly without loading bytes into a user slice prematurely.
func TestRead_FullRecord(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payload := []byte("kage-events")
	off := appendOne(t, seg, payload)

	size := int32(recordHeaderSize + len(payload))
	r, err := seg.Read(off, size)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	got := readAll(t, r)
	if len(got) != int(size) {
		t.Fatalf("len(got) = %d, want %d", len(got), size)
	}

	// First 4 bytes: big-endian payload length.
	gotLen := uint32(got[0])<<24 | uint32(got[1])<<16 | uint32(got[2])<<8 | uint32(got[3])
	if gotLen != uint32(len(payload)) {
		t.Errorf("header length = %d, want %d", gotLen, len(payload))
	}
	if string(got[recordHeaderSize:]) != string(payload) {
		t.Errorf("payload = %q, want %q", got[recordHeaderSize:], payload)
	}
}

// TestRead_PayloadWindow verifies a sub-range (payload only, skipping header).
func TestRead_PayloadWindow(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payload := []byte("just the payload")
	off := appendOne(t, seg, payload)

	payloadOff := off + recordHeaderSize
	r, err := seg.Read(payloadOff, int32(len(payload)))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(readAll(t, r)) != string(payload) {
		t.Errorf("payload mismatch")
	}
}

// TestRead_MultipleRecords verifies independent readers for different records.
func TestRead_MultipleRecords(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payloads := [][]byte{[]byte("first"), []byte("second record"), []byte("third")}

	type rec struct {
		off  uint64
		size int32
		want []byte
	}
	var recs []rec
	for _, p := range payloads {
		off := appendOne(t, seg, p)
		recs = append(recs, rec{off: off, size: int32(recordHeaderSize + len(p)), want: p})
	}

	for i, res := range recs {
		r, err := seg.Read(res.off, res.size)
		if err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}
		got := readAll(t, r)
		if string(got[recordHeaderSize:]) != string(res.want) {
			t.Errorf("record[%d] = %q, want %q", i, got[recordHeaderSize:], res.want)
		}
	}
}

// TestRead_FlushesBufferedData verifies that data written but not yet flushed
// to the OS file is still visible through the returned SectionReader.
func TestRead_FlushesBufferedData(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payload := []byte("buffered")
	off := appendOne(t, seg, payload)

	// Do NOT call seg.Flush() — Read must flush internally.
	size := int32(recordHeaderSize + len(payload))
	r, err := seg.Read(off, size)
	if err != nil {
		t.Fatalf("Read without prior Flush: %v", err)
	}
	got := readAll(t, r)
	if string(got[recordHeaderSize:]) != string(payload) {
		t.Errorf("got %q, want %q", got[recordHeaderSize:], payload)
	}
}

// TestRead_IoCopy verifies io.Copy from the SectionReader works correctly.
// On Linux (production target inside Docker), Go's net package will substitute
// this copy with sendfile(2) when the destination is a net.Conn, because
// *io.SectionReader's underlying *os.File implements syscall.Conn.
func TestRead_IoCopy(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payload := []byte("sendfile-candidate")
	off := appendOne(t, seg, payload)

	size := int32(recordHeaderSize + len(payload))
	r, err := seg.Read(off, size)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	var dst bytes.Buffer
	n, err := io.Copy(&dst, r)
	if err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if n != int64(size) {
		t.Errorf("copied %d bytes, want %d", n, size)
	}
	if string(dst.Bytes()[recordHeaderSize:]) != string(payload) {
		t.Errorf("payload mismatch after io.Copy")
	}
}

// TestRead_SectionReaderSize verifies Seek(0, SeekEnd) reports the window size.
func TestRead_SectionReaderSize(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payload := []byte("size-check")
	off := appendOne(t, seg, payload)
	size := int32(recordHeaderSize + len(payload))

	r, err := seg.Read(off, size)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	sr := r.(*io.SectionReader)
	end, err := sr.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if end != int64(size) {
		t.Errorf("SectionReader size = %d, want %d", end, size)
	}
}

// TestRead_ErrInvalidSize_Zero verifies size=0 returns ErrInvalidSize.
func TestRead_ErrInvalidSize_Zero(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	appendOne(t, seg, []byte("x"))

	_, err := seg.Read(0, 0)
	if !errors.Is(err, ErrInvalidSize) {
		t.Errorf("want ErrInvalidSize, got %v", err)
	}
}

// TestRead_ErrInvalidSize_Negative verifies negative size returns ErrInvalidSize.
func TestRead_ErrInvalidSize_Negative(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	appendOne(t, seg, []byte("x"))

	_, err := seg.Read(0, -1)
	if !errors.Is(err, ErrInvalidSize) {
		t.Errorf("want ErrInvalidSize, got %v", err)
	}
}

// TestRead_ErrInvalidOffset verifies offset ≥ written size returns ErrInvalidOffset.
func TestRead_ErrInvalidOffset(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	appendOne(t, seg, []byte("hello"))

	_, err := seg.Read(9999, 1)
	if !errors.Is(err, ErrInvalidOffset) {
		t.Errorf("want ErrInvalidOffset, got %v", err)
	}
}

// TestRead_ErrInvalidSize_Overflow verifies offset+size > written returns ErrInvalidSize.
func TestRead_ErrInvalidSize_Overflow(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)
	payload := []byte("hello")
	off := appendOne(t, seg, payload)

	_, err := seg.Read(off, int32(recordHeaderSize+len(payload)+1))
	if !errors.Is(err, ErrInvalidSize) {
		t.Errorf("want ErrInvalidSize, got %v", err)
	}
}

// TestRead_EmptySegment verifies offset=0 on an empty segment returns ErrInvalidOffset.
func TestRead_EmptySegment(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)

	_, err := seg.Read(0, 1)
	if !errors.Is(err, ErrInvalidOffset) {
		t.Errorf("want ErrInvalidOffset on empty segment, got %v", err)
	}
}

// TestRead_Concurrent verifies concurrent Read calls all return correct data.
func TestRead_Concurrent(t *testing.T) {
	seg := openDefault(t, tempDir(t), 0)

	const records = 20
	type rec struct {
		off     uint64
		size    int32
		payload []byte
	}
	recs := make([]rec, records)
	for i := range recs {
		p := bytes.Repeat([]byte{byte('a' + i)}, 32)
		off := appendOne(t, seg, p)
		recs[i] = rec{off: off, size: int32(recordHeaderSize + len(p)), payload: p}
	}
	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	var wg sync.WaitGroup
	for i := range recs {
		wg.Add(1)
		go func(r rec) {
			defer wg.Done()
			reader, err := seg.Read(r.off, r.size)
			if err != nil {
				t.Errorf("concurrent Read: %v", err)
				return
			}
			got := make([]byte, r.size)
			if _, err := io.ReadFull(reader, got); err != nil {
				t.Errorf("concurrent ReadFull: %v", err)
				return
			}
			if string(got[recordHeaderSize:]) != string(r.payload) {
				t.Errorf("concurrent payload mismatch")
			}
		}(recs[i])
	}
	wg.Wait()
}
