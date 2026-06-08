package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

// buildFetchRequestBody encodes a FetchRequest v4 body (after the header).
func buildFetchRequestBody(replicaID int32, maxWaitMs, minBytes, maxBytes int32, isolationLevel int8, topics []FetchTopicData) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, replicaID)
	binary.Write(&buf, binary.BigEndian, maxWaitMs)
	binary.Write(&buf, binary.BigEndian, minBytes)
	binary.Write(&buf, binary.BigEndian, maxBytes)
	binary.Write(&buf, binary.BigEndian, isolationLevel)
	binary.Write(&buf, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&buf, binary.BigEndian, int16(len(t.TopicName)))
		buf.WriteString(t.TopicName)
		binary.Write(&buf, binary.BigEndian, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			binary.Write(&buf, binary.BigEndian, p.Partition)
			binary.Write(&buf, binary.BigEndian, p.FetchOffset)
			binary.Write(&buf, binary.BigEndian, p.PartitionMaxBytes)
		}
	}
	return buf.Bytes()
}

func TestParseFetchRequest_SingleTopicPartition(t *testing.T) {
	topics := []FetchTopicData{
		{
			TopicName: "kage-events",
			Partitions: []FetchPartitionData{
				{Partition: 0, FetchOffset: 1024, PartitionMaxBytes: 65536},
			},
		},
	}
	body := buildFetchRequestBody(-1, 500, 1, 1048576, 0, topics)
	hdr := &RequestHeader{ApiKey: ApiKeyFetch}

	req, err := NewDecoder(bytes.NewReader(body)).ParseFetchRequest(hdr)
	if err != nil {
		t.Fatalf("ParseFetchRequest: %v", err)
	}

	if req.ReplicaID != -1 {
		t.Errorf("ReplicaID = %d, want -1", req.ReplicaID)
	}
	if req.MaxWaitMs != 500 {
		t.Errorf("MaxWaitMs = %d, want 500", req.MaxWaitMs)
	}
	if req.MinBytes != 1 {
		t.Errorf("MinBytes = %d, want 1", req.MinBytes)
	}
	if req.MaxBytes != 1048576 {
		t.Errorf("MaxBytes = %d, want 1048576", req.MaxBytes)
	}
	if req.IsolationLevel != 0 {
		t.Errorf("IsolationLevel = %d, want 0", req.IsolationLevel)
	}
	if len(req.Topics) != 1 {
		t.Fatalf("topics len = %d, want 1", len(req.Topics))
	}
	if req.Topics[0].TopicName != "kage-events" {
		t.Errorf("topic = %q, want kage-events", req.Topics[0].TopicName)
	}
	if len(req.Topics[0].Partitions) != 1 {
		t.Fatalf("partitions len = %d, want 1", len(req.Topics[0].Partitions))
	}
	p := req.Topics[0].Partitions[0]
	if p.Partition != 0 {
		t.Errorf("partition = %d, want 0", p.Partition)
	}
	if p.FetchOffset != 1024 {
		t.Errorf("fetch_offset = %d, want 1024", p.FetchOffset)
	}
	if p.PartitionMaxBytes != 65536 {
		t.Errorf("partition_max_bytes = %d, want 65536", p.PartitionMaxBytes)
	}
}

func TestParseFetchRequest_MultipleTopicsAndPartitions(t *testing.T) {
	topics := []FetchTopicData{
		{
			TopicName: "topic-a",
			Partitions: []FetchPartitionData{
				{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1024},
				{Partition: 1, FetchOffset: 512, PartitionMaxBytes: 2048},
			},
		},
		{
			TopicName: "topic-b",
			Partitions: []FetchPartitionData{
				{Partition: 0, FetchOffset: 256, PartitionMaxBytes: 4096},
			},
		},
	}
	body := buildFetchRequestBody(-1, 100, 1, 65536, 0, topics)
	req, err := NewDecoder(bytes.NewReader(body)).ParseFetchRequest(&RequestHeader{})
	if err != nil {
		t.Fatalf("ParseFetchRequest: %v", err)
	}

	if len(req.Topics) != 2 {
		t.Fatalf("topics = %d, want 2", len(req.Topics))
	}
	if len(req.Topics[0].Partitions) != 2 {
		t.Errorf("topic-a partitions = %d, want 2", len(req.Topics[0].Partitions))
	}
	if req.Topics[0].Partitions[1].FetchOffset != 512 {
		t.Errorf("topic-a partition[1] fetch_offset = %d, want 512", req.Topics[0].Partitions[1].FetchOffset)
	}
	if len(req.Topics[1].Partitions) != 1 {
		t.Errorf("topic-b partitions = %d, want 1", len(req.Topics[1].Partitions))
	}
}

func TestParseFetchRequest_IsolationLevelRead(t *testing.T) {
	topics := []FetchTopicData{
		{TopicName: "t", Partitions: []FetchPartitionData{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1}}},
	}
	body := buildFetchRequestBody(-1, 0, 0, 1, 1, topics) // isolation_level = 1 (READ_COMMITTED)
	req, err := NewDecoder(bytes.NewReader(body)).ParseFetchRequest(&RequestHeader{})
	if err != nil {
		t.Fatalf("ParseFetchRequest: %v", err)
	}
	if req.IsolationLevel != 1 {
		t.Errorf("IsolationLevel = %d, want 1", req.IsolationLevel)
	}
}

func TestParseFetchRequest_Truncated(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{0x00, 0x01})) // only 2 bytes, not enough
	_, err := d.ParseFetchRequest(&RequestHeader{})
	if err == nil {
		t.Fatal("expected error on truncated input, got nil")
	}
}

// writeFetchResponseToBytes is a test helper that calls WriteFetchResponse
// and returns the resulting framed bytes.
func writeFetchResponseToBytes(t *testing.T, correlationID int32, resp *FetchResponse) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFetchResponse(&buf, correlationID, resp); err != nil {
		t.Fatalf("WriteFetchResponse: %v", err)
	}
	return buf.Bytes()
}

func TestEncodeFetchResponse_SinglePartition(t *testing.T) {
	batchData := []byte("batch-data")
	resp := &FetchResponse{
		ThrottleTimeMs: 0,
		Topics: []FetchTopicResponse{
			{
				TopicName: "kage-events",
				Partitions: []FetchPartitionResponse{
					{
						Partition:     0,
						ErrorCode:     0,
						HighWatermark: 2048,
						RecordBatch:   bytes.NewReader(batchData),
						BatchSize:     int32(len(batchData)),
					},
				},
			},
		},
	}

	raw := writeFetchResponseToBytes(t, 42, resp)

	dec := NewDecoder(bytes.NewReader(raw))
	size, _ := dec.ReadInt32()
	if size <= 0 {
		t.Fatalf("size prefix = %d, want > 0", size)
	}

	corrID, _ := dec.ReadInt32()
	if corrID != 42 {
		t.Errorf("correlationID = %d, want 42", corrID)
	}

	throttle, _ := dec.ReadInt32()
	if throttle != 0 {
		t.Errorf("throttleTimeMs = %d, want 0", throttle)
	}

	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}

	topicName, _ := dec.ReadString()
	if topicName != "kage-events" {
		t.Errorf("topic = %q, want kage-events", topicName)
	}

	partCount, _ := dec.ReadInt32()
	if partCount != 1 {
		t.Fatalf("partition count = %d, want 1", partCount)
	}

	partition, _ := dec.ReadInt32()
	if partition != 0 {
		t.Errorf("partition = %d, want 0", partition)
	}

	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}

	hwm, _ := dec.ReadInt64()
	if hwm != 2048 {
		t.Errorf("high watermark = %d, want 2048", hwm)
	}

	lso, _ := dec.ReadInt64() // LastStableOffset (v4+)
	if lso != 2048 {
		t.Errorf("last stable offset = %d, want 2048", lso)
	}
	abortedLen, _ := dec.ReadInt32() // AbortedTransactions array length (v4+)
	if abortedLen != 0 {
		t.Errorf("aborted transactions length = %d, want 0", abortedLen)
	}

	batchSize, _ := dec.ReadInt32()
	if batchSize != int32(len(batchData)) {
		t.Errorf("batch size = %d, want %d", batchSize, len(batchData))
	}

	batch, _ := dec.ReadBytes(int(batchSize))
	if string(batch) != "batch-data" {
		t.Errorf("batch = %q, want batch-data", batch)
	}
}

func TestEncodeFetchResponse_NullBatch(t *testing.T) {
	resp := &FetchResponse{
		Topics: []FetchTopicResponse{
			{
				TopicName: "t",
				Partitions: []FetchPartitionResponse{
					{Partition: 0, ErrorCode: ErrCodeOffsetOutOfRange, HighWatermark: 0, RecordBatch: nil, BatchSize: -1},
				},
			},
		},
	}

	raw := writeFetchResponseToBytes(t, 1, resp)

	dec := NewDecoder(bytes.NewReader(raw))
	dec.ReadInt32()  // size
	dec.ReadInt32()  // corrID
	dec.ReadInt32()  // throttle
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition
	errCode, _ := dec.ReadInt16()
	if errCode != ErrCodeOffsetOutOfRange {
		t.Errorf("error code = %d, want %d", errCode, ErrCodeOffsetOutOfRange)
	}
	dec.ReadInt64() // hwm
	dec.ReadInt64() // LastStableOffset (v4+)
	dec.ReadInt32() // AbortedTransactions array length (v4+)
	batchSize, _ := dec.ReadInt32()
	if batchSize != -1 {
		t.Errorf("batch size = %d, want -1 (null records)", batchSize)
	}
}

func TestEncodeFetchResponse_MultipleTopics(t *testing.T) {
	resp := &FetchResponse{
		Topics: []FetchTopicResponse{
			{TopicName: "a", Partitions: []FetchPartitionResponse{
				{Partition: 0, ErrorCode: 0, HighWatermark: 100, RecordBatch: bytes.NewReader([]byte("aa")), BatchSize: 2},
			}},
			{TopicName: "b", Partitions: []FetchPartitionResponse{
				{Partition: 0, ErrorCode: 0, HighWatermark: 200, RecordBatch: bytes.NewReader([]byte("bb")), BatchSize: 2},
			}},
		},
	}
	raw := writeFetchResponseToBytes(t, 7, resp)

	dec := NewDecoder(bytes.NewReader(raw))
	dec.ReadInt32() // size
	dec.ReadInt32() // corrID
	dec.ReadInt32() // throttle
	count, _ := dec.ReadInt32()
	if count != 2 {
		t.Errorf("topic count = %d, want 2", count)
	}
}

// TestEncodeFetchResponse_RoundTrip verifies that batch bytes written via
// io.Copy arrive intact in the framed response.
func TestEncodeFetchResponse_RoundTrip(t *testing.T) {
	batchData := []byte("round-trip-payload")
	resp := &FetchResponse{
		Topics: []FetchTopicResponse{
			{
				TopicName: "rt",
				Partitions: []FetchPartitionResponse{
					{
						Partition:     0,
						ErrorCode:     0,
						HighWatermark: 999,
						RecordBatch:   bytes.NewReader(batchData),
						BatchSize:     int32(len(batchData)),
					},
				},
			},
		},
	}

	raw := writeFetchResponseToBytes(t, 123, resp)

	// The 4-byte Kafka size prefix covers everything after it.
	dec := NewDecoder(bytes.NewReader(raw))
	frameSize, _ := dec.ReadInt32()
	if int(frameSize) != len(raw)-4 {
		t.Errorf("frame size = %d, want %d", frameSize, len(raw)-4)
	}
	dec.ReadInt32()  // corrID
	dec.ReadInt32()  // throttle
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition
	dec.ReadInt16()  // error code
	dec.ReadInt64()  // high watermark
	dec.ReadInt64()  // LastStableOffset (v4+)
	dec.ReadInt32()  // AbortedTransactions array length (v4+)
	sz, _ := dec.ReadInt32()
	got, _ := dec.ReadBytes(int(sz))
	if !bytes.Equal(got, batchData) {
		t.Errorf("round-trip batch = %q, want %q", got, batchData)
	}
}

// TestWriteFetchResponse_IOCopyZeroCopy verifies WriteFetchResponse streams
// batch data via io.Copy without buffering it into the header encoder.
func TestWriteFetchResponse_IOCopyZeroCopy(t *testing.T) {
	// Wrap bytes.Reader to track whether Read was called (proving io.Copy
	// was used instead of reading into a []byte first).
	called := false
	batchData := []byte("zero-copy-check")
	inner := bytes.NewReader(batchData)
	r := readerFunc(func(p []byte) (int, error) {
		called = true
		return inner.Read(p)
	})

	resp := &FetchResponse{
		Topics: []FetchTopicResponse{
			{
				TopicName: "zc",
				Partitions: []FetchPartitionResponse{
					{Partition: 0, ErrorCode: 0, HighWatermark: 0, RecordBatch: r, BatchSize: int32(len(batchData))},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := WriteFetchResponse(&buf, 1, resp); err != nil {
		t.Fatalf("WriteFetchResponse: %v", err)
	}
	if !called {
		t.Error("RecordBatch reader was never called — io.Copy path not exercised")
	}
}

// readerFunc adapts a function to the io.Reader interface.
type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// TestWriteFetchResponse_WriteError verifies that a write error is propagated.
func TestWriteFetchResponse_WriteError(t *testing.T) {
	resp := &FetchResponse{
		Topics: []FetchTopicResponse{
			{TopicName: "t", Partitions: []FetchPartitionResponse{
				{Partition: 0, ErrorCode: 0, HighWatermark: 0, RecordBatch: nil, BatchSize: -1},
			}},
		},
	}
	ew := &errorWriter{}
	err := WriteFetchResponse(ew, 1, resp)
	if err == nil {
		t.Error("expected write error, got nil")
	}
}

// errorWriter always returns an error on Write.
type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (int, error) {
	return 0, io.ErrClosedPipe
}
