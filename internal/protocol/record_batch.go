package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// ErrInvalidMagicByte is returned when the RecordBatch magic byte is not 2.
var ErrInvalidMagicByte = errors.New("record batch: invalid magic byte (expected 2)")

// ErrCRCMismatch is returned when the computed CRC32C does not match the
// CRC stored in the RecordBatch header.
var ErrCRCMismatch = errors.New("record batch: CRC mismatch — possible data corruption")

// Kafka RecordBatch v2 header layout (all fields big-endian):
//
//	Offset  Size  Field
//	     0     8  BaseOffset
//	     8     4  Length            (byte count from PartitionLeaderEpoch to end)
//	    12     4  PartitionLeaderEpoch
//	    16     1  MagicByte         (must be 2)
//	    17     4  CRC               (CRC32C of bytes [21, Length+12])
//	    21     2  Attributes
//	    23     4  LastOffsetDelta
//	    27     8  FirstTimestamp
//	    35     8  MaxTimestamp
//	    43     8  ProducerId
//	    51     2  ProducerEpoch
//	    53     4  BaseSequence
//	    57     4  RecordsCount
//	    61     …  Records…
const (
	rbOffsetBaseOffset   = 0
	rbOffsetLength       = 8
	rbOffsetMagic        = 16
	rbOffsetCRC          = 17
	rbOffsetCRCBody      = 21 // CRC covers bytes from here to end of batch
	rbOffsetRecordsCount = 57
	rbMinBytes           = 61 // minimum valid batch size
)

// crc32cTable is the Castagnoli polynomial table used by Kafka for CRC32C.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ValidateRecordBatch performs a shallow parse of a Kafka RecordBatch (magic v2).
//
// It checks:
//   - The batch is at least rbMinBytes long.
//   - MagicByte == 2.
//   - The CRC32C stored in bytes [17,21) matches the CRC32C computed over
//     bytes [21, Length+12).
//
// On success it returns the RecordsCount field from the batch header.
func ValidateRecordBatch(batch []byte) (recordCount int32, err error) {
	if len(batch) < rbMinBytes {
		return 0, fmt.Errorf("record batch: too short (%d bytes, need at least %d)", len(batch), rbMinBytes)
	}

	if batch[rbOffsetMagic] != 2 {
		return 0, fmt.Errorf("%w: got %d", ErrInvalidMagicByte, batch[rbOffsetMagic])
	}

	// Length field covers PartitionLeaderEpoch (offset 12) through end of batch.
	// Total batch size on wire = Length + 12 (BaseOffset(8) + Length(4)).
	batchLength := int(binary.BigEndian.Uint32(batch[rbOffsetLength:]))
	expectedTotal := batchLength + 12
	if len(batch) < expectedTotal {
		return 0, fmt.Errorf("record batch: batch length field (%d) exceeds buffer size (%d)", expectedTotal, len(batch))
	}

	// Stored CRC: bytes [17, 21)
	storedCRC := binary.BigEndian.Uint32(batch[rbOffsetCRC:])

	// CRC body: bytes [21, Length+12)
	computedCRC := crc32.Checksum(batch[rbOffsetCRCBody:expectedTotal], crc32cTable)

	if storedCRC != computedCRC {
		return 0, fmt.Errorf("%w: stored=0x%08x computed=0x%08x", ErrCRCMismatch, storedCRC, computedCRC)
	}

	recordCount = int32(binary.BigEndian.Uint32(batch[rbOffsetRecordsCount:]))
	return recordCount, nil
}
