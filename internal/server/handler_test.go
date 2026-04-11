package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/codexorange/kage/internal/metrics"
	"github.com/codexorange/kage/internal/protocol"
	"github.com/codexorange/kage/internal/storage"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// mockStore is a test double for storage.Store.
type mockStore struct {
	nextOffset  uint64
	failWith    error
	appended    [][]byte
	fetchData   []byte // bytes returned by Read
	fetchErr    error  // error returned by Read
	totalSize   int64  // value returned by Size
}

func (m *mockStore) Append(data []byte) (uint64, error) {
	if m.failWith != nil {
		return 0, m.failWith
	}
	off := m.nextOffset
	m.nextOffset += uint64(len(data)) + 4 // mimic recordHeaderSize
	m.appended = append(m.appended, data)
	return off, nil
}

func (m *mockStore) Read(fetchOffset uint64, maxBytes int32) (io.Reader, int32, error) {
	if m.fetchErr != nil {
		return nil, 0, m.fetchErr
	}
	data := m.fetchData
	if int32(len(data)) > maxBytes {
		data = data[:maxBytes]
	}
	return bytes.NewReader(data), int32(len(data)), nil
}

func (m *mockStore) Size() int64 {
	return m.totalSize
}

// newTestHandler returns a Handler with a mock store, silent logger, and real metrics.
func newTestHandler(store *mockStore) *Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(logger, store, metrics.New())
}

// buildFrame wraps a body in a 4-byte size-prefixed frame (the Kafka wire framing).
func buildFrame(body []byte) []byte {
	var frame bytes.Buffer
	binary.Write(&frame, binary.BigEndian, int32(len(body)))
	frame.Write(body)
	return frame.Bytes()
}

// buildRequestHeader encodes ApiKey/ApiVersion/CorrelationID/ClientID.
func buildRequestHeader(apiKey, apiVersion int16, correlationID int32, clientID string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, apiKey)
	binary.Write(&buf, binary.BigEndian, apiVersion)
	binary.Write(&buf, binary.BigEndian, correlationID)
	binary.Write(&buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
	return buf.Bytes()
}

// buildMetadataRequestFrame builds a complete on-wire MetadataRequest frame.
func buildMetadataRequestFrame(correlationID int32, clientID string, topics []string) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyMetadata, 0, correlationID, clientID))
	binary.Write(&body, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(t)))
		body.WriteString(t)
	}
	return buildFrame(body.Bytes())
}

// buildProduceRequestFrame builds a complete ProduceRequest frame.
func buildProduceRequestFrame(correlationID int32, clientID string, acks int16, topics []protocol.ProduceTopicData) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyProduce, 3, correlationID, clientID))

	// TransactionalID: null (-1)
	binary.Write(&body, binary.BigEndian, int16(-1))
	binary.Write(&body, binary.BigEndian, acks)
	binary.Write(&body, binary.BigEndian, int32(5000)) // timeoutMs
	binary.Write(&body, binary.BigEndian, int32(len(topics)))

	for _, t := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(t.TopicName)))
		body.WriteString(t.TopicName)
		binary.Write(&body, binary.BigEndian, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			binary.Write(&body, binary.BigEndian, p.Partition)
			binary.Write(&body, binary.BigEndian, int32(len(p.RecordBatch)))
			body.Write(p.RecordBatch)
		}
	}
	return buildFrame(body.Bytes())
}

// readResponse reads a length-prefixed response frame and returns the body.
func readResponse(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	var size int32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		t.Fatalf("read response size: %v", err)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return body
}

// ── Metadata tests ────────────────────────────────────────────────────────────

func TestHandler_MetadataRequest(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	go newTestHandler(&mockStore{}).Handle(serverConn)

	clientConn.Write(buildMetadataRequestFrame(99, "test-client", []string{"kage-events"}))

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))

	corrID, _ := dec.ReadInt32()
	if corrID != 99 {
		t.Errorf("correlationID = %d, want 99", corrID)
	}
	brokerCount, _ := dec.ReadInt32()
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1", brokerCount)
	}
	dec.ReadInt32()  // NodeID
	dec.ReadString() // host
	dec.ReadInt32()  // port

	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Errorf("topic count = %d, want 1", topicCount)
	}
	dec.ReadInt16() // error code
	name, _ := dec.ReadString()
	if name != "kage-events" {
		t.Errorf("topic name = %q, want kage-events", name)
	}
}

// ── Produce tests ─────────────────────────────────────────────────────────────

func TestHandler_ProduceRequest_AcksLeader(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{}
	go newTestHandler(store).Handle(serverConn)

	batch := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	frame := buildProduceRequestFrame(42, "producer-1", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "kage-events", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: batch},
		}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))

	corrID, _ := dec.ReadInt32()
	if corrID != 42 {
		t.Errorf("correlationID = %d, want 42", corrID)
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
	baseOffset, _ := dec.ReadInt64()
	if baseOffset != 0 {
		t.Errorf("base offset = %d, want 0", baseOffset)
	}

	// Verify the batch was actually stored.
	if len(store.appended) != 1 {
		t.Fatalf("store.appended len = %d, want 1", len(store.appended))
	}
	if !bytes.Equal(store.appended[0], batch) {
		t.Errorf("stored batch mismatch: got %v, want %v", store.appended[0], batch)
	}
}

func TestHandler_ProduceRequest_AcksAll(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{}
	go newTestHandler(store).Handle(serverConn)

	frame := buildProduceRequestFrame(10, "p", protocol.AcksAll, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("payload")},
		}},
	})
	clientConn.Write(frame)

	// AcksAll is treated as AcksLeader (no replication yet) — response expected.
	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	corrID, _ := dec.ReadInt32()
	if corrID != 10 {
		t.Errorf("correlationID = %d, want 10", corrID)
	}
}

func TestHandler_ProduceRequest_AcksNone_NoResponse(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	store := &mockStore{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		newTestHandler(store).Handle(serverConn)
	}()

	frame := buildProduceRequestFrame(7, "p", protocol.AcksNone, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("fire-and-forget")},
		}},
	})
	clientConn.Write(frame)

	// No response should arrive — verify by timeout.
	clientConn.SetDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 1)
	_, err := clientConn.Read(buf)
	if err == nil {
		t.Fatal("expected no response for acks=0, but read succeeded")
	}

	// Close the connection so the handler exits, then wait for it.
	clientConn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}

	// Batch must still be stored.
	if len(store.appended) != 1 {
		t.Errorf("store.appended = %d, want 1", len(store.appended))
	}
}

func TestHandler_ProduceRequest_StorageError_ReportsErrorCode(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{failWith: errors.New("disk full")}
	go newTestHandler(store).Handle(serverConn)

	frame := buildProduceRequestFrame(55, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("data")},
		}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	dec.ReadInt32() // correlationID
	dec.ReadInt32() // topic count
	dec.ReadString() // topic name
	dec.ReadInt32() // partition count
	dec.ReadInt32() // partition index

	errCode, _ := dec.ReadInt16()
	if errCode == 0 {
		t.Error("expected non-zero error code on storage failure")
	}
}

func TestHandler_ProduceRequest_MultiplePartitions(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{}
	go newTestHandler(store).Handle(serverConn)

	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "kage-events", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("batch-0")},
			{Partition: 1, RecordBatch: []byte("batch-1")},
		}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	dec.ReadInt32() // correlationID
	dec.ReadInt32() // topic count
	dec.ReadString() // topic name

	partCount, _ := dec.ReadInt32()
	if partCount != 2 {
		t.Errorf("partition count = %d, want 2", partCount)
	}
	if len(store.appended) != 2 {
		t.Errorf("stored batches = %d, want 2", len(store.appended))
	}
}

// ── Unsupported ApiKey ────────────────────────────────────────────────────────

func TestHandler_UnsupportedApiKey(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		newTestHandler(&mockStore{}).Handle(serverConn)
	}()

	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(99)) // ApiKey
	binary.Write(&body, binary.BigEndian, int16(0))  // ApiVersion
	binary.Write(&body, binary.BigEndian, int32(1))  // CorrelationID
	binary.Write(&body, binary.BigEndian, int16(0))  // ClientID (empty)
	clientConn.Write(buildFrame(body.Bytes()))

	clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err := clientConn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
}

// ── Fetch helpers ─────────────────────────────────────────────────────────────

type fetchPartition struct {
	partition         int32
	fetchOffset       int64
	partitionMaxBytes int32
}

// buildFetchRequestFrame encodes a FetchRequest v4 on-wire frame.
func buildFetchRequestFrame(correlationID int32, clientID string, maxBytes int32, topics map[string][]fetchPartition) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyFetch, 4, correlationID, clientID))

	binary.Write(&body, binary.BigEndian, int32(-1))  // ReplicaId
	binary.Write(&body, binary.BigEndian, int32(500)) // MaxWaitMs
	binary.Write(&body, binary.BigEndian, int32(1))   // MinBytes
	binary.Write(&body, binary.BigEndian, maxBytes)   // MaxBytes
	body.WriteByte(0)                                  // IsolationLevel (int8)
	binary.Write(&body, binary.BigEndian, int32(len(topics)))

	for topicName, partitions := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(topicName)))
		body.WriteString(topicName)
		binary.Write(&body, binary.BigEndian, int32(len(partitions)))
		for _, p := range partitions {
			binary.Write(&body, binary.BigEndian, p.partition)
			binary.Write(&body, binary.BigEndian, p.fetchOffset)
			binary.Write(&body, binary.BigEndian, p.partitionMaxBytes)
		}
	}
	return buildFrame(body.Bytes())
}

// decodeFetchResponse reads and partially decodes a FetchResponse for testing.
type fetchPartitionResult struct {
	partition     int32
	errorCode     int16
	highWatermark int64
	batchSize     int32
	batch         []byte
}

type fetchTopicResult struct {
	topicName  string
	partitions []fetchPartitionResult
}

func decodeFetchResponse(t *testing.T, body []byte) (corrID int32, throttleMs int32, topics []fetchTopicResult) {
	t.Helper()
	dec := protocol.NewDecoder(bytes.NewReader(body))

	corrID, _ = dec.ReadInt32()
	throttleMs, _ = dec.ReadInt32()
	topicCount, _ := dec.ReadInt32()

	topics = make([]fetchTopicResult, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		name, _ := dec.ReadString()
		partCount, _ := dec.ReadInt32()
		parts := make([]fetchPartitionResult, 0, partCount)
		for j := int32(0); j < partCount; j++ {
			partition, _ := dec.ReadInt32()
			errCode, _ := dec.ReadInt16()
			hwm, _ := dec.ReadInt64()
			batchSize, _ := dec.ReadInt32()
			var batch []byte
			if batchSize > 0 {
				batch, _ = dec.ReadBytes(int(batchSize))
			}
			parts = append(parts, fetchPartitionResult{
				partition:     partition,
				errorCode:     errCode,
				highWatermark: hwm,
				batchSize:     batchSize,
				batch:         batch,
			})
		}
		topics = append(topics, fetchTopicResult{topicName: name, partitions: parts})
	}
	return
}

// ── Fetch tests ───────────────────────────────────────────────────────────────

func TestHandler_FetchRequest_ReturnsData(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	payload := []byte("record-batch-content")
	store := &mockStore{
		fetchData: payload,
		totalSize: int64(len(payload)),
	}
	go newTestHandler(store).Handle(serverConn)

	frame := buildFetchRequestFrame(11, "consumer-1", 1024, map[string][]fetchPartition{
		"kage-events": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	corrID, _, topics := decodeFetchResponse(t, body)

	if corrID != 11 {
		t.Errorf("correlationID = %d, want 11", corrID)
	}
	if len(topics) != 1 {
		t.Fatalf("topic count = %d, want 1", len(topics))
	}
	if topics[0].topicName != "kage-events" {
		t.Errorf("topic = %q, want kage-events", topics[0].topicName)
	}
	if len(topics[0].partitions) != 1 {
		t.Fatalf("partition count = %d, want 1", len(topics[0].partitions))
	}
	p := topics[0].partitions[0]
	if p.errorCode != 0 {
		t.Errorf("error code = %d, want 0", p.errorCode)
	}
	if !bytes.Equal(p.batch, payload) {
		t.Errorf("batch = %v, want %v", p.batch, payload)
	}
}

func TestHandler_FetchRequest_MaxBytesCapApplied(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	payload := []byte("0123456789abcdef") // 16 bytes
	store := &mockStore{
		fetchData: payload,
		totalSize: int64(len(payload)),
	}
	go newTestHandler(store).Handle(serverConn)

	// Global MaxBytes = 8 — must cap what the store returns.
	frame := buildFetchRequestFrame(22, "c", 8, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	if len(topics[0].partitions[0].batch) > 8 {
		t.Errorf("batch size = %d, want ≤ 8 (MaxBytes cap)", len(topics[0].partitions[0].batch))
	}
}

func TestHandler_FetchRequest_PartitionMaxBytesCap(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	payload := []byte("0123456789abcdef") // 16 bytes
	store := &mockStore{
		fetchData: payload,
		totalSize: int64(len(payload)),
	}
	go newTestHandler(store).Handle(serverConn)

	// PartitionMaxBytes = 4, GlobalMaxBytes = 1024.
	frame := buildFetchRequestFrame(33, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 4}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	if len(topics[0].partitions[0].batch) > 4 {
		t.Errorf("batch size = %d, want ≤ 4 (PartitionMaxBytes cap)", len(topics[0].partitions[0].batch))
	}
}

func TestHandler_FetchRequest_OffsetOutOfRange(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{
		fetchErr:  storage.ErrInvalidOffset,
		totalSize: 0,
	}
	go newTestHandler(store).Handle(serverConn)

	frame := buildFetchRequestFrame(44, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 9999, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	p := topics[0].partitions[0]
	if p.errorCode != protocol.ErrCodeOffsetOutOfRange {
		t.Errorf("error code = %d, want %d (ErrCodeOffsetOutOfRange)", p.errorCode, protocol.ErrCodeOffsetOutOfRange)
	}
}

func TestHandler_FetchRequest_HighWatermark(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	const hwm = int64(4096)
	store := &mockStore{
		fetchData: []byte("data"),
		totalSize: hwm,
	}
	go newTestHandler(store).Handle(serverConn)

	frame := buildFetchRequestFrame(55, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	if topics[0].partitions[0].highWatermark != hwm {
		t.Errorf("high watermark = %d, want %d", topics[0].partitions[0].highWatermark, hwm)
	}
}

func TestHandler_FetchRequest_GlobalBudgetExhausted(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	payload := []byte("hello") // 5 bytes
	store := &mockStore{
		fetchData: payload,
		totalSize: int64(len(payload)),
	}
	go newTestHandler(store).Handle(serverConn)

	// MaxBytes = 5 — first partition consumes it all; second gets nothing.
	// We use a single topic with two partitions but maps are unordered,
	// so use separate topics to guarantee ordering.
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyFetch, 4, 66, "c"))
	binary.Write(&body, binary.BigEndian, int32(-1)) // ReplicaId
	binary.Write(&body, binary.BigEndian, int32(500))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int32(5)) // MaxBytes = 5
	body.WriteByte(0)                               // IsolationLevel
	binary.Write(&body, binary.BigEndian, int32(1)) // 1 topic

	topicName := "t"
	binary.Write(&body, binary.BigEndian, int16(len(topicName)))
	body.WriteString(topicName)
	binary.Write(&body, binary.BigEndian, int32(2)) // 2 partitions

	// Partition 0: partitionMaxBytes = 1024
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int64(0))
	binary.Write(&body, binary.BigEndian, int32(1024))
	// Partition 1: partitionMaxBytes = 1024
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int64(0))
	binary.Write(&body, binary.BigEndian, int32(1024))

	clientConn.Write(buildFrame(body.Bytes()))

	respBody := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, respBody)

	if len(topics) != 1 {
		t.Fatalf("topic count = %d, want 1", len(topics))
	}
	parts := topics[0].partitions
	if len(parts) != 2 {
		t.Fatalf("partition count = %d, want 2", len(parts))
	}
	// First partition got data; second should have empty/nil batch (budget exhausted).
	if len(parts[0].batch) == 0 {
		t.Error("partition 0: expected non-empty batch")
	}
	if len(parts[1].batch) != 0 {
		t.Errorf("partition 1: expected empty batch (budget exhausted), got %d bytes", len(parts[1].batch))
	}
}

func TestHandler_FetchRequest_CorrelationID(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{fetchData: []byte("x"), totalSize: 1}
	go newTestHandler(store).Handle(serverConn)

	frame := buildFetchRequestFrame(999, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	corrID, _, _ := decodeFetchResponse(t, body)
	if corrID != 999 {
		t.Errorf("correlationID = %d, want 999", corrID)
	}
}

// TestHandler_FetchRequest_StorageError_ReportsErrorCode verifies that a
// non-ErrInvalidOffset storage error is still reported as ErrCodeOffsetOutOfRange.
func TestHandler_FetchRequest_StorageError_ReportsErrorCode(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	store := &mockStore{
		fetchErr:  errors.New("disk i/o error"),
		totalSize: 100,
	}
	go newTestHandler(store).Handle(serverConn)

	frame := buildFetchRequestFrame(77, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	p := topics[0].partitions[0]
	if p.errorCode == 0 {
		t.Error("expected non-zero error code on storage failure")
	}
}
