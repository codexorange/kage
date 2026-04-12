package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

// buildRecordBatch builds a minimal but valid Kafka RecordBatch v2 containing
// recordCount records.  The Records payload is just zeroed bytes of recordCount
// bytes so that we have something after the header without needing full record
// encoding.
//
// Layout written:
//
//	BaseOffset(8) Length(4) PartitionLeaderEpoch(4) MagicByte(1)
//	CRC(4) Attributes(2) LastOffsetDelta(4) FirstTimestamp(8)
//	MaxTimestamp(8) ProducerId(8) ProducerEpoch(2) BaseSequence(4)
//	RecordsCount(4) Records(recordCount bytes)
func buildRecordBatch(recordCount int32) []byte {
	// Build the CRC body first (everything from byte 21 onward).
	var crcBody [40]byte // fixed fields after the CRC
	binary.BigEndian.PutUint16(crcBody[0:], 0)          // Attributes
	binary.BigEndian.PutUint32(crcBody[2:], uint32(recordCount-1)) // LastOffsetDelta
	binary.BigEndian.PutUint64(crcBody[6:], 0)          // FirstTimestamp
	binary.BigEndian.PutUint64(crcBody[14:], 0)         // MaxTimestamp
	binary.BigEndian.PutUint64(crcBody[22:], 0)         // ProducerId
	binary.BigEndian.PutUint16(crcBody[30:], 0)         // ProducerEpoch
	binary.BigEndian.PutUint32(crcBody[32:], 0)         // BaseSequence
	binary.BigEndian.PutUint32(crcBody[36:], uint32(recordCount)) // RecordsCount

	// Records payload: just recordCount zero bytes as a stub.
	records := make([]byte, recordCount)

	crcInput := append(crcBody[:], records...)
	checksum := crc32.Checksum(crcInput, crc32cTable)

	// Length = PartitionLeaderEpoch(4) + MagicByte(1) + CRC(4) + len(crcInput)
	length := 4 + 1 + 4 + len(crcInput)

	buf := make([]byte, 12+length)
	binary.BigEndian.PutUint64(buf[0:], 0)                   // BaseOffset
	binary.BigEndian.PutUint32(buf[8:], uint32(length))      // Length
	binary.BigEndian.PutUint32(buf[12:], 0)                  // PartitionLeaderEpoch
	buf[16] = 2                                               // MagicByte
	binary.BigEndian.PutUint32(buf[17:], checksum)           // CRC
	copy(buf[21:], crcInput)

	return buf
}

func TestValidateRecordBatch_Valid(t *testing.T) {
	batch := buildRecordBatch(3)
	count, err := ValidateRecordBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if count != 3 {
		t.Errorf("record count = %d, want 3", count)
	}
}

func TestValidateRecordBatch_SingleRecord(t *testing.T) {
	batch := buildRecordBatch(1)
	count, err := ValidateRecordBatch(batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("record count = %d, want 1", count)
	}
}

func TestValidateRecordBatch_TooShort(t *testing.T) {
	_, err := ValidateRecordBatch(make([]byte, 10))
	if err == nil {
		t.Fatal("expected error for too-short batch, got nil")
	}
}

func TestValidateRecordBatch_WrongMagicByte(t *testing.T) {
	batch := buildRecordBatch(1)
	batch[rbOffsetMagic] = 1 // tamper: set to v1
	_, err := ValidateRecordBatch(batch)
	if err == nil {
		t.Fatal("expected error for wrong magic byte, got nil")
	}
	if !errors.Is(err, ErrInvalidMagicByte) {
		t.Errorf("expected ErrInvalidMagicByte in chain, got: %v", err)
	}
}

func TestValidateRecordBatch_CRCMismatch(t *testing.T) {
	batch := buildRecordBatch(2)
	// Flip one bit in the records payload (last byte) to corrupt the CRC.
	batch[len(batch)-1] ^= 0xFF
	_, err := ValidateRecordBatch(batch)
	if err == nil {
		t.Fatal("expected CRC error, got nil")
	}
	if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("expected ErrCRCMismatch in chain, got: %v", err)
	}
}

func TestValidateRecordBatch_TamperedCRCField(t *testing.T) {
	batch := buildRecordBatch(1)
	// Overwrite the stored CRC with a wrong value.
	binary.BigEndian.PutUint32(batch[rbOffsetCRC:], 0xDEADBEEF)
	_, err := ValidateRecordBatch(batch)
	if err == nil {
		t.Fatal("expected CRC error, got nil")
	}
	if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("expected ErrCRCMismatch in chain, got: %v", err)
	}
}

func TestValidateRecordBatch_LengthExceedsBuffer(t *testing.T) {
	batch := buildRecordBatch(1)
	// Set Length to a huge value so expectedTotal > len(batch).
	binary.BigEndian.PutUint32(batch[rbOffsetLength:], 0x7FFFFFFF)
	_, err := ValidateRecordBatch(batch)
	if err == nil {
		t.Fatal("expected error when length exceeds buffer, got nil")
	}
}

func TestValidateRecordBatch_EmptyBatch(t *testing.T) {
	_, err := ValidateRecordBatch([]byte{})
	if err == nil {
		t.Fatal("expected error for empty slice, got nil")
	}
}
