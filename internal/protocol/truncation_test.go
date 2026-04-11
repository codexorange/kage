package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// buildFullRequestHeaderBytes builds the raw bytes for a request header
// (without the 4-byte size prefix).
func buildFullRequestHeaderBytes(apiKey int16, corrID int32, clientID string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(0))          // size (placeholder)
	binary.Write(&buf, binary.BigEndian, apiKey)            // ApiKey
	binary.Write(&buf, binary.BigEndian, int16(0))          // ApiVersion
	binary.Write(&buf, binary.BigEndian, corrID)            // CorrelationID
	binary.Write(&buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
	return buf.Bytes()
}

// TestParseRequestHeader_TruncatedMidFields covers error paths for truncation
// after the first field (size already covered by existing tests).
func TestParseRequestHeader_TruncatedMidFields(t *testing.T) {
	full := buildFullRequestHeaderBytes(0, 1, "c")
	// Truncate at various offsets after the first field (4 bytes):
	// offset 4 = missing ApiKey; 6 = missing ApiVersion; 8 = missing CorrID; 12 = missing ClientID length
	for _, cutAt := range []int{4, 6, 8, 12} {
		_, err := NewDecoder(bytes.NewReader(full[:cutAt])).ParseRequestHeader()
		if err == nil {
			t.Errorf("cutAt=%d: expected error, got nil", cutAt)
		}
	}
}

// TestParseMetadataRequest_TruncatedTopicName covers the error path when the
// topic name body is missing.
func TestParseMetadataRequest_TruncatedTopicName(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(1))  // topic count = 1
	binary.Write(&buf, binary.BigEndian, int16(10)) // string length = 10 but no body

	_, err := NewDecoder(&buf).ParseMetadataRequest(&RequestHeader{})
	if err == nil {
		t.Fatal("expected error on truncated topic name, got nil")
	}
}

// TestParseProduceRequest_TruncatedMidParse covers error returns after the
// first field in ParseProduceRequest.
func TestParseProduceRequest_TruncatedMidParse(t *testing.T) {
	// Build a fully valid body and truncate at each field boundary.
	var full bytes.Buffer
	binary.Write(&full, binary.BigEndian, int16(-1))    // txnID: null
	binary.Write(&full, binary.BigEndian, int16(1))     // acks
	binary.Write(&full, binary.BigEndian, int32(5000))  // timeoutMs
	binary.Write(&full, binary.BigEndian, int32(1))     // topic count
	binary.Write(&full, binary.BigEndian, int16(5))     // topic name len
	full.WriteString("topic")
	binary.Write(&full, binary.BigEndian, int32(1))     // partition count
	binary.Write(&full, binary.BigEndian, int32(0))     // partition index
	binary.Write(&full, binary.BigEndian, int32(3))     // batch size
	full.WriteString("abc")

	data := full.Bytes()
	// Truncate after: txnID(2), acks(4), timeoutMs(8), topicCount(12), topicNameLen(14), partCount(21), partIdx(25), batchSize(29)
	for _, cutAt := range []int{2, 4, 8, 12, 14, 21, 25, 29} {
		_, err := NewDecoder(bytes.NewReader(data[:cutAt])).ParseProduceRequest(&RequestHeader{})
		if err == nil {
			t.Errorf("cutAt=%d: expected error, got nil", cutAt)
		}
	}
}

// TestParseProduceRequest_NegativeBatchSize covers the explicit negative-batch-size
// error path.
func TestParseProduceRequest_NegativeBatchSize(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int16(-1))   // txnID: null
	binary.Write(&buf, binary.BigEndian, int16(1))    // acks
	binary.Write(&buf, binary.BigEndian, int32(5000)) // timeoutMs
	binary.Write(&buf, binary.BigEndian, int32(1))    // topic count
	binary.Write(&buf, binary.BigEndian, int16(1))    // topic name len
	buf.WriteByte('t')
	binary.Write(&buf, binary.BigEndian, int32(1))    // partition count
	binary.Write(&buf, binary.BigEndian, int32(0))    // partition
	binary.Write(&buf, binary.BigEndian, int32(-5))   // negative batch size

	_, err := NewDecoder(&buf).ParseProduceRequest(&RequestHeader{})
	if err == nil {
		t.Fatal("expected error for negative batch size, got nil")
	}
}

// TestParseFetchRequest_TruncatedMidParse covers error returns at each field
// boundary inside ParseFetchRequest.
func TestParseFetchRequest_TruncatedMidParse(t *testing.T) {
	var full bytes.Buffer
	binary.Write(&full, binary.BigEndian, int32(-1))   // replicaID
	binary.Write(&full, binary.BigEndian, int32(500))  // maxWaitMs
	binary.Write(&full, binary.BigEndian, int32(1))    // minBytes
	binary.Write(&full, binary.BigEndian, int32(1<<20)) // maxBytes
	binary.Write(&full, binary.BigEndian, int8(0))     // isolationLevel
	binary.Write(&full, binary.BigEndian, int32(1))    // topic count
	binary.Write(&full, binary.BigEndian, int16(5))    // topic name len
	full.WriteString("topic")
	binary.Write(&full, binary.BigEndian, int32(1))    // partition count
	binary.Write(&full, binary.BigEndian, int32(0))    // partition
	binary.Write(&full, binary.BigEndian, int64(0))    // fetchOffset
	binary.Write(&full, binary.BigEndian, int32(1024)) // partitionMaxBytes

	data := full.Bytes()
	// Truncate at: replicaID(4), maxWaitMs(8), minBytes(12), maxBytes(16),
	// isolationLevel(17), topicCount(21), topicNameLen(23), partCount(30), partition(34), fetchOffset(42)
	for _, cutAt := range []int{4, 8, 12, 16, 17, 21, 23, 30, 34, 42} {
		_, err := NewDecoder(bytes.NewReader(data[:cutAt])).ParseFetchRequest(&RequestHeader{})
		if err == nil {
			t.Errorf("cutAt=%d: expected error, got nil", cutAt)
		}
	}
}
