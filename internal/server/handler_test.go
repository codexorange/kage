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

	"github.com/codexorange/kage/internal/protocol"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// mockStore is a test double for storage.AppendStore.
type mockStore struct {
	nextOffset uint64
	failWith   error
	appended   [][]byte
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

// newTestHandler returns a Handler with a mock store and a silent logger.
func newTestHandler(store *mockStore) *Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(logger, store)
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
