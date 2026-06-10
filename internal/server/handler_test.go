package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codexorange/kage/internal/metrics"
	"github.com/codexorange/kage/internal/protocol"
	"github.com/codexorange/kage/internal/storage"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// tempDir creates a temporary directory that is cleaned up after the test.
func tempDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "kage-handler-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// newTestBrokerStore opens a real BrokerStore in a temp directory.
func newTestBrokerStore(t *testing.T) *storage.BrokerStore {
	t.Helper()
	bs, err := storage.OpenBrokerStore(
		context.Background(),
		tempDir(t),
		storage.SegmentConfig{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("OpenBrokerStore: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

// newTestHandler returns a Handler wired to a fresh BrokerStore.
func newTestHandler(t *testing.T) (*Handler, *storage.BrokerStore) {
	t.Helper()
	bs := newTestBrokerStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(logger, bs, metrics.New()), bs
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
	return buildMetadataRequestFrameV(correlationID, clientID, topics, 6)
}

// buildMetadataRequestFrameV builds a MetadataRequest frame with the given API version.
func buildMetadataRequestFrameV(correlationID int32, clientID string, topics []string, version int16) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyMetadata, version, correlationID, clientID))
	binary.Write(&body, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(t)))
		body.WriteString(t)
	}
	if version >= 4 {
		body.WriteByte(1) // allow_auto_topic_creation = true
	}
	return buildFrame(body.Bytes())
}

// buildValidRecordBatch wraps payload bytes inside a valid Kafka RecordBatch v2
// frame (magic=2, CRC32C correct, RecordsCount=1).  All tests that go through
// handleProduce must use this so that ValidateRecordBatch passes.
func buildValidRecordBatch(payload []byte) []byte {
	// CRC body: fixed header fields (40 bytes) + payload as the single record.
	var fixedFields [40]byte
	binary.BigEndian.PutUint16(fixedFields[0:], 0)  // Attributes
	binary.BigEndian.PutUint32(fixedFields[2:], 0)  // LastOffsetDelta (0 = 1 record)
	binary.BigEndian.PutUint64(fixedFields[6:], 0)  // FirstTimestamp
	binary.BigEndian.PutUint64(fixedFields[14:], 0) // MaxTimestamp
	binary.BigEndian.PutUint64(fixedFields[22:], 0) // ProducerId
	binary.BigEndian.PutUint16(fixedFields[30:], 0) // ProducerEpoch
	binary.BigEndian.PutUint32(fixedFields[32:], 0) // BaseSequence
	binary.BigEndian.PutUint32(fixedFields[36:], 1) // RecordsCount = 1

	crcInput := append(fixedFields[:], payload...)
	checksum := crc32.Checksum(crcInput, crc32.MakeTable(crc32.Castagnoli))

	// length = PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + len(crcInput)
	length := 4 + 1 + 4 + len(crcInput)
	buf := make([]byte, 12+length)
	binary.BigEndian.PutUint64(buf[0:], 0)              // BaseOffset
	binary.BigEndian.PutUint32(buf[8:], uint32(length)) // Length
	binary.BigEndian.PutUint32(buf[12:], 0)             // PartitionLeaderEpoch
	buf[16] = 2                                         // MagicByte
	binary.BigEndian.PutUint32(buf[17:], checksum)      // CRC
	copy(buf[21:], crcInput)
	return buf
}

// buildListOffsetsRequestFrame builds a complete ListOffsetsRequest v1 frame.
func buildListOffsetsRequestFrame(correlationID int32, clientID string, topics []protocol.ListOffsetsTopicRequest) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyListOffsets, 1, correlationID, clientID))
	binary.Write(&body, binary.BigEndian, int32(-1)) // replicaID = -1 (consumer)
	binary.Write(&body, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(t.TopicName)))
		body.WriteString(t.TopicName)
		binary.Write(&body, binary.BigEndian, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			binary.Write(&body, binary.BigEndian, p.Partition)
			binary.Write(&body, binary.BigEndian, p.Timestamp)
		}
	}
	return buildFrame(body.Bytes())
}

// buildProduceRequestFrame builds a complete ProduceRequest v2 frame.
// Each partition's RecordBatch is wrapped in a valid RecordBatch v2 envelope.
// v2 layout: Acks(int16) | TimeoutMs(int32) | topics[] — no transactional_id.
func buildProduceRequestFrame(correlationID int32, clientID string, acks int16, topics []protocol.ProduceTopicData) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyProduce, 2, correlationID, clientID))

	binary.Write(&body, binary.BigEndian, acks)
	binary.Write(&body, binary.BigEndian, int32(5000)) // timeoutMs
	binary.Write(&body, binary.BigEndian, int32(len(topics)))

	for _, t := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(t.TopicName)))
		body.WriteString(t.TopicName)
		binary.Write(&body, binary.BigEndian, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			batch := buildValidRecordBatch(p.RecordBatch)
			binary.Write(&body, binary.BigEndian, p.Partition)
			binary.Write(&body, binary.BigEndian, int32(len(batch)))
			body.Write(batch)
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

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildMetadataRequestFrame(99, "test-client", []string{"kage-events"}))

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))

	corrID, _ := dec.ReadInt32()
	if corrID != 99 {
		t.Errorf("correlationID = %d, want 99", corrID)
	}
	dec.ReadInt32() // ThrottleTimeMs (v1+)
	brokerCount, _ := dec.ReadInt32()
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1", brokerCount)
	}
	dec.ReadInt32()  // NodeID
	dec.ReadString() // host
	dec.ReadInt32()  // port
	dec.ReadInt16()  // Rack (nullable string, -1 = null)
	dec.ReadInt16()  // ClusterID (nullable string, -1 = null)
	dec.ReadInt32()  // ControllerID

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

// TestHandler_MetadataRequest_EmptyTopics verifies that an empty topics list
// returns all known partitions.
func TestHandler_MetadataRequest_EmptyTopics(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, bs := newTestHandler(t)
	// Pre-create two partitions so Topics() is non-empty.
	bs.GetOrCreatePartition("alpha", 0)
	bs.GetOrCreatePartition("beta", 0)
	go h.Handle(serverConn)

	clientConn.Write(buildMetadataRequestFrame(1, "c", []string{}))

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	dec.ReadInt32()  // corrID
	dec.ReadInt32()  // ThrottleTimeMs (v1+)
	dec.ReadInt32()  // broker count
	dec.ReadInt32()  // NodeID
	dec.ReadString() // host
	dec.ReadInt32()  // port
	dec.ReadInt16()  // Rack (nullable string, -1 = null)
	dec.ReadInt16()  // ClusterID (nullable string, -1 = null)
	dec.ReadInt32()  // ControllerID

	topicCount, _ := dec.ReadInt32()
	if topicCount != 2 {
		t.Errorf("topic count = %d, want 2", topicCount)
	}
}

// TestHandler_MetadataRequest_DynamicTopicCreation verifies that asking for a
// topic that does not exist causes it to be created.
func TestHandler_MetadataRequest_DynamicTopicCreation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, bs := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildMetadataRequestFrame(2, "c", []string{"new-topic"}))
	readResponse(t, clientConn) // consume response

	tps := bs.Topics()
	found := false
	for _, tp := range tps {
		if tp.Topic == "new-topic" && tp.Partition == 0 {
			found = true
		}
	}
	if !found {
		t.Error("expected 'new-topic' partition 0 to be created by metadata request")
	}
}

// ── Produce tests ─────────────────────────────────────────────────────────────

func TestHandler_ProduceRequest_AcksLeader(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

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
}

func TestHandler_ProduceRequest_AcksAll(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

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

	h, _ := newTestHandler(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(serverConn)
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
}

func TestHandler_ProduceRequest_MultiplePartitions(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "kage-events", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("batch-0")},
			{Partition: 1, RecordBatch: []byte("batch-1")},
		}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	dec.ReadInt32()  // correlationID
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name

	partCount, _ := dec.ReadInt32()
	if partCount != 2 {
		t.Errorf("partition count = %d, want 2", partCount)
	}
}

// TestHandler_ProduceRequest_OffsetSequencing verifies that successive produce
// calls on the same partition return advancing offsets.
func TestHandler_ProduceRequest_OffsetSequencing(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	send := func(batch []byte) int64 {
		frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
			{TopicName: "t", Partitions: []protocol.ProducePartitionData{
				{Partition: 0, RecordBatch: batch},
			}},
		})
		clientConn.Write(frame)
		body := readResponse(t, clientConn)
		dec := protocol.NewDecoder(bytes.NewReader(body))
		dec.ReadInt32() // corrID
		dec.ReadInt32() // topics
		dec.ReadString()
		dec.ReadInt32() // partitions
		dec.ReadInt32() // partition
		dec.ReadInt16() // errCode
		off, _ := dec.ReadInt64()
		return off
	}

	off1 := send([]byte("aaa"))
	off2 := send([]byte("bb"))
	if off1 != 0 {
		t.Errorf("first offset = %d, want 0", off1)
	}
	if off2 <= off1 {
		t.Errorf("second offset %d must be greater than first %d", off2, off1)
	}
}

// ── Unsupported ApiKey ────────────────────────────────────────────────────────

func TestHandler_UnsupportedApiKey(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(serverConn)
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
	body.WriteByte(0)                                 // IsolationLevel (int8)
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
			dec.ReadInt64() // LastStableOffset (v4+)
			dec.ReadInt32() // AbortedTransactions array length (v4+)
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

// produceAndFetch is a helper that produces a batch then fetches it back.
func produceAndFetch(t *testing.T, clientConn net.Conn, topic string, payload []byte, maxBytes int32) []fetchPartitionResult {
	t.Helper()
	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: topic, Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: payload},
		}},
	})
	clientConn.Write(frame)
	readResponse(t, clientConn) // consume produce response

	fetchFrame := buildFetchRequestFrame(2, "c", maxBytes, map[string][]fetchPartition{
		topic: {{partition: 0, fetchOffset: 0, partitionMaxBytes: maxBytes}},
	})
	clientConn.Write(fetchFrame)
	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)
	if len(topics) == 0 {
		t.Fatal("no topics in fetch response")
	}
	return topics[0].partitions
}

// ── Fetch tests ───────────────────────────────────────────────────────────────

func TestHandler_FetchRequest_ReturnsData(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	payload := []byte("record-batch-content")
	parts := produceAndFetch(t, clientConn, "kage-events", payload, 1024)

	if len(parts) != 1 {
		t.Fatalf("partition count = %d, want 1", len(parts))
	}
	p := parts[0]
	if p.errorCode != 0 {
		t.Errorf("error code = %d, want 0", p.errorCode)
	}
	// The stored record is framed with a 4-byte length header; the batch
	// bytes start 4 bytes into p.batch.
	if len(p.batch) < len(payload) {
		t.Errorf("batch too short: got %d bytes, want at least %d", len(p.batch), len(payload))
	}
}

func TestHandler_FetchRequest_OffsetOutOfRange(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Fetch from an empty partition — offset 9999 is out of range.
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

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	payload := []byte("data")
	parts := produceAndFetch(t, clientConn, "t", payload, 1024)

	// HWM = segment recordHeaderSize(4) + RecordBatch envelope size.
	// Envelope = BaseOffset(8)+Length(4)+PLEpoch(4)+Magic(1)+CRC(4)+fixedFields(40)+payload
	envelopeSize := int64(8 + 4 + 4 + 1 + 4 + 40 + len(payload))
	expectedHWM := int64(4) + envelopeSize // 4-byte segment record header
	hwm := parts[0].highWatermark
	if hwm != expectedHWM {
		t.Errorf("high watermark = %d, want %d", hwm, expectedHWM)
	}
}

func TestHandler_FetchRequest_MaxBytesCapApplied(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	payload := []byte("0123456789abcdef") // 16 bytes

	// Produce first.
	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: payload},
		}},
	})
	clientConn.Write(frame)
	readResponse(t, clientConn)

	// Fetch with global MaxBytes = 8.
	fetchFrame := buildFetchRequestFrame(22, "c", 8, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(fetchFrame)
	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	if len(topics[0].partitions[0].batch) > 8 {
		t.Errorf("batch size = %d, want ≤ 8 (MaxBytes cap)", len(topics[0].partitions[0].batch))
	}
}

func TestHandler_FetchRequest_PartitionMaxBytesCap(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	payload := []byte("0123456789abcdef") // 16 bytes

	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: payload},
		}},
	})
	clientConn.Write(frame)
	readResponse(t, clientConn)

	// Fetch with PartitionMaxBytes = 4.
	fetchFrame := buildFetchRequestFrame(33, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 4}},
	})
	clientConn.Write(fetchFrame)
	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	if len(topics[0].partitions[0].batch) > 4 {
		t.Errorf("batch size = %d, want ≤ 4 (PartitionMaxBytes cap)", len(topics[0].partitions[0].batch))
	}
}

func TestHandler_FetchRequest_GlobalBudgetExhausted(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Produce 5 bytes into partition 0 so fetch has something to return.
	payload := []byte("hello")
	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: payload},
			{Partition: 1, RecordBatch: payload},
		}},
	})
	clientConn.Write(frame)
	readResponse(t, clientConn)

	// MaxBytes = total record size (4+5=9). First partition consumes the budget.
	recordSize := int32(4 + len(payload)) // recordHeaderSize + payload
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyFetch, 4, 66, "c"))
	binary.Write(&body, binary.BigEndian, int32(-1)) // ReplicaId
	binary.Write(&body, binary.BigEndian, int32(500))
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, recordSize) // MaxBytes = exactly one record
	body.WriteByte(0)                                 // IsolationLevel
	binary.Write(&body, binary.BigEndian, int32(1))   // 1 topic

	topicName := "t"
	binary.Write(&body, binary.BigEndian, int16(len(topicName)))
	body.WriteString(topicName)
	binary.Write(&body, binary.BigEndian, int32(2)) // 2 partitions

	// Partition 0
	binary.Write(&body, binary.BigEndian, int32(0))
	binary.Write(&body, binary.BigEndian, int64(0))
	binary.Write(&body, binary.BigEndian, int32(1024))
	// Partition 1
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

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Produce first so there's something to fetch.
	prodFrame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("x")},
		}},
	})
	clientConn.Write(prodFrame)
	readResponse(t, clientConn)

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

// ── BrokerStore / dynamic topic tests ─────────────────────────────────────────

// TestHandler_ProduceAndFetch_MultipleTopics verifies that produce and fetch
// correctly route data to separate per-topic partition stores.
func TestHandler_ProduceAndFetch_MultipleTopics(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Produce to two different topics.
	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "topic-a", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("alpha")},
		}},
		{TopicName: "topic-b", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("beta")},
		}},
	})
	clientConn.Write(frame)
	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	dec.ReadInt32() // corrID
	topicCount, _ := dec.ReadInt32()
	if topicCount != 2 {
		t.Fatalf("produce topic count = %d, want 2", topicCount)
	}
}

// TestHandler_BrokerStore_Persistence verifies that topic partitions created
// during produce are discoverable via Topics().
func TestHandler_BrokerStore_Persistence(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, bs := newTestHandler(t)
	go h.Handle(serverConn)

	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "sensor-data", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("reading-1")},
		}},
	})
	clientConn.Write(frame)
	readResponse(t, clientConn)

	tps := bs.Topics()
	found := false
	for _, tp := range tps {
		if tp.Topic == "sensor-data" && tp.Partition == 0 {
			found = true
		}
	}
	if !found {
		t.Error("expected 'sensor-data' partition 0 in broker store topics")
	}
}

// TestOpenBrokerStore_Discovery verifies that existing partition directories
// are loaded when the store is reopened.
func TestOpenBrokerStore_Discovery(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()
	cfg := storage.SegmentConfig{}

	// Create two partition directories by opening and closing a store.
	bs, err := storage.OpenBrokerStore(ctx, dir, cfg, logger)
	if err != nil {
		t.Fatalf("first OpenBrokerStore: %v", err)
	}
	bs.GetOrCreatePartition("events", 0)
	bs.GetOrCreatePartition("events", 1)
	bs.Close()

	// Reopen and verify discovery.
	bs2, err := storage.OpenBrokerStore(ctx, dir, cfg, logger)
	if err != nil {
		t.Fatalf("second OpenBrokerStore: %v", err)
	}
	defer bs2.Close()

	tps := bs2.Topics()
	found := map[int32]bool{}
	for _, tp := range tps {
		if tp.Topic == "events" {
			found[tp.Partition] = true
		}
	}
	if !found[0] || !found[1] {
		t.Errorf("expected partitions 0 and 1 for 'events', got %v", found)
	}
}

// TestHandler_ProduceRequest_StorageError_ReportsErrorCode verifies that a
// segment append error is reported as a non-zero error code.
// We force the error by using a MaxSize so small no record can fit.
func TestHandler_ProduceRequest_StorageError_ReportsErrorCode(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// MaxSize = 4: header alone is 4 bytes so any payload causes ErrSegmentFull
	// on every append (rollover also fails since fresh segment is also too small).
	bs, err := storage.OpenBrokerStore(ctx, dir, storage.SegmentConfig{MaxSize: 4}, logger)
	if err != nil {
		t.Fatalf("OpenBrokerStore: %v", err)
	}
	defer bs.Close()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h := NewHandler(logger, bs, metrics.New())
	go h.Handle(serverConn)

	frame := buildProduceRequestFrame(55, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("data")},
		}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))
	dec.ReadInt32()  // correlationID
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition index

	errCode, _ := dec.ReadInt16()
	if errCode == 0 {
		t.Error("expected non-zero error code on storage failure")
	}
}

var ctx = context.Background()

// TestHandler_FetchRequest_CaughtUp verifies that fetching at exactly the
// HighWatermark (consumer is caught up) returns success with an empty batch,
// not ErrCodeOffsetOutOfRange.
func TestHandler_FetchRequest_CaughtUp(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Empty partition: Size()=0, so FetchOffset=0 == HighWatermark=0.
	frame := buildFetchRequestFrame(77, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	p := topics[0].partitions[0]
	if p.errorCode != 0 {
		t.Errorf("error code = %d, want 0 (caught up is not an error)", p.errorCode)
	}
	if p.batchSize != -1 {
		t.Errorf("batch size = %d, want -1 (empty batch when caught up)", p.batchSize)
	}
}

// TestHandler_FetchRequest_FutureOffset verifies that fetching beyond the
// HighWatermark returns ErrCodeOffsetOutOfRange.
func TestHandler_FetchRequest_FutureOffset(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Partition is empty (HWM=0); requesting offset 999 is a genuine future offset.
	frame := buildFetchRequestFrame(78, "c", 1024, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 999, partitionMaxBytes: 1024}},
	})
	clientConn.Write(frame)

	body := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, body)

	p := topics[0].partitions[0]
	if p.errorCode != protocol.ErrCodeOffsetOutOfRange {
		t.Errorf("error code = %d, want %d (ErrCodeOffsetOutOfRange)", p.errorCode, protocol.ErrCodeOffsetOutOfRange)
	}
}

// TestHandler_FetchRequest_MultiplePartitionsIsolated verifies that fetching
// from two partitions of the same topic returns independent data.
func TestHandler_FetchRequest_MultiplePartitionsIsolated(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Produce distinct payloads into partition 0 and partition 1.
	frame := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "events", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("part0-data")},
			{Partition: 1, RecordBatch: []byte("part1-data")},
		}},
	})
	clientConn.Write(frame)
	readResponse(t, clientConn)

	// Both partitions must exist with non-zero data.
	var fetchBody bytes.Buffer
	fetchBody.Write(buildRequestHeader(protocol.ApiKeyFetch, 4, 2, "c"))
	binary.Write(&fetchBody, binary.BigEndian, int32(-1))
	binary.Write(&fetchBody, binary.BigEndian, int32(500))
	binary.Write(&fetchBody, binary.BigEndian, int32(1))
	binary.Write(&fetchBody, binary.BigEndian, int32(65536)) // MaxBytes
	fetchBody.WriteByte(0)
	binary.Write(&fetchBody, binary.BigEndian, int32(1)) // 1 topic

	tn := "events"
	binary.Write(&fetchBody, binary.BigEndian, int16(len(tn)))
	fetchBody.WriteString(tn)
	binary.Write(&fetchBody, binary.BigEndian, int32(2)) // 2 partitions
	for _, pid := range []int32{0, 1} {
		binary.Write(&fetchBody, binary.BigEndian, pid)
		binary.Write(&fetchBody, binary.BigEndian, int64(0))
		binary.Write(&fetchBody, binary.BigEndian, int32(1024))
	}
	clientConn.Write(buildFrame(fetchBody.Bytes()))

	respBody := readResponse(t, clientConn)
	_, _, topics := decodeFetchResponse(t, respBody)

	if len(topics) != 1 {
		t.Fatalf("topic count = %d, want 1", len(topics))
	}
	parts := topics[0].partitions
	if len(parts) != 2 {
		t.Fatalf("partition count = %d, want 2", len(parts))
	}
	for _, p := range parts {
		if len(p.batch) == 0 {
			t.Errorf("partition %d: expected non-empty batch", p.partition)
		}
		if p.errorCode != 0 {
			t.Errorf("partition %d: expected error code 0, got %d", p.partition, p.errorCode)
		}
	}
}

// Verify that errors.Is is still importable in this package (used by handler.go).
var _ = errors.New

// ── OffsetCommit / OffsetFetch helpers ────────────────────────────────────────

// buildOffsetCommitFrame encodes an OffsetCommitRequest v2 on-wire frame.
func buildOffsetCommitFrame(correlationID int32, clientID, groupID string, topic string, partition int32, offset int64) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyOffsetCommit, 2, correlationID, clientID))

	// GroupID
	binary.Write(&body, binary.BigEndian, int16(len(groupID)))
	body.WriteString(groupID)
	// GenerationID
	binary.Write(&body, binary.BigEndian, int32(-1))
	// MemberID (empty)
	binary.Write(&body, binary.BigEndian, int16(0))
	// RetentionTimeMs (-1 = use broker default)
	binary.Write(&body, binary.BigEndian, int64(-1))
	// topics array: 1 topic
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int16(len(topic)))
	body.WriteString(topic)
	// partitions array: 1 partition
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, partition)
	binary.Write(&body, binary.BigEndian, offset)
	// CommittedMetadata: empty string
	binary.Write(&body, binary.BigEndian, int16(0))

	return buildFrame(body.Bytes())
}

// buildOffsetFetchFrame encodes an OffsetFetchRequest v1 on-wire frame.
func buildOffsetFetchFrame(correlationID int32, clientID, groupID string, topic string, partition int32) []byte {
	var body bytes.Buffer
	body.Write(buildRequestHeader(protocol.ApiKeyOffsetFetch, 1, correlationID, clientID))

	binary.Write(&body, binary.BigEndian, int16(len(groupID)))
	body.WriteString(groupID)
	// topics array: 1 topic
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, int16(len(topic)))
	body.WriteString(topic)
	// partitions array: 1 partition
	binary.Write(&body, binary.BigEndian, int32(1))
	binary.Write(&body, binary.BigEndian, partition)

	return buildFrame(body.Bytes())
}

// decodeOffsetCommitResponse parses an OffsetCommitResponse body.
func decodeOffsetCommitResponse(t *testing.T, body []byte) (corrID int32, errCode int16) {
	t.Helper()
	dec := protocol.NewDecoder(bytes.NewReader(body))
	corrID, _ = dec.ReadInt32()
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition index
	errCode, _ = dec.ReadInt16()
	return
}

// decodeOffsetFetchResponse parses an OffsetFetchResponse body and returns the
// committed offset for the first partition of the first topic.
func decodeOffsetFetchResponse(t *testing.T, body []byte) (corrID int32, committedOffset int64, errCode int16) {
	t.Helper()
	dec := protocol.NewDecoder(bytes.NewReader(body))
	corrID, _ = dec.ReadInt32()
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition index
	committedOffset, _ = dec.ReadInt64()
	dec.ReadString() // metadata
	errCode, _ = dec.ReadInt16()
	return
}

// ── OffsetCommit tests ────────────────────────────────────────────────────────

// TestHandler_OffsetCommit_StoresOffset verifies that a committed offset is
// persisted to __consumer_offsets and returned by a subsequent OffsetFetch.
func TestHandler_OffsetCommit_StoresOffset(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Commit offset 42 for group "my-group", topic "events", partition 0.
	clientConn.Write(buildOffsetCommitFrame(1, "c", "my-group", "events", 0, 42))
	body := readResponse(t, clientConn)

	_, errCode := decodeOffsetCommitResponse(t, body)
	if errCode != 0 {
		t.Fatalf("offset commit error code = %d, want 0", errCode)
	}

	// Fetch the committed offset back.
	clientConn.Write(buildOffsetFetchFrame(2, "c", "my-group", "events", 0))
	body = readResponse(t, clientConn)

	_, offset, fetchErrCode := decodeOffsetFetchResponse(t, body)
	if fetchErrCode != 0 {
		t.Fatalf("offset fetch error code = %d, want 0", fetchErrCode)
	}
	if offset != 42 {
		t.Errorf("committed offset = %d, want 42", offset)
	}
}

// TestHandler_OffsetFetch_UnknownGroup returns OffsetUnknown (-1) for a group
// that has never committed.
func TestHandler_OffsetFetch_UnknownGroup(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildOffsetFetchFrame(3, "c", "ghost-group", "events", 0))
	body := readResponse(t, clientConn)

	_, offset, errCode := decodeOffsetFetchResponse(t, body)
	if errCode != 0 {
		t.Fatalf("error code = %d, want 0", errCode)
	}
	if offset != protocol.OffsetUnknown {
		t.Errorf("offset = %d, want %d (OffsetUnknown)", offset, protocol.OffsetUnknown)
	}
}

// TestHandler_OffsetCommit_OverwritesOffset verifies that committing a new
// offset for the same group/topic/partition overwrites the previous one.
func TestHandler_OffsetCommit_OverwritesOffset(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildOffsetCommitFrame(1, "c", "grp", "t", 0, 10))
	readResponse(t, clientConn) // consume first commit response

	clientConn.Write(buildOffsetCommitFrame(2, "c", "grp", "t", 0, 99))
	readResponse(t, clientConn) // consume second commit response

	clientConn.Write(buildOffsetFetchFrame(3, "c", "grp", "t", 0))
	body := readResponse(t, clientConn)

	_, offset, _ := decodeOffsetFetchResponse(t, body)
	if offset != 99 {
		t.Errorf("offset = %d, want 99 (latest commit)", offset)
	}
}

// TestHandler_OffsetCommit_IsolatedByGroup verifies two consumer groups
// committing to the same topic-partition maintain independent offsets.
func TestHandler_OffsetCommit_IsolatedByGroup(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildOffsetCommitFrame(1, "c", "group-a", "t", 0, 111))
	readResponse(t, clientConn)

	clientConn.Write(buildOffsetCommitFrame(2, "c", "group-b", "t", 0, 222))
	readResponse(t, clientConn)

	clientConn.Write(buildOffsetFetchFrame(3, "c", "group-a", "t", 0))
	bodyA := readResponse(t, clientConn)
	_, offsetA, _ := decodeOffsetFetchResponse(t, bodyA)

	clientConn.Write(buildOffsetFetchFrame(4, "c", "group-b", "t", 0))
	bodyB := readResponse(t, clientConn)
	_, offsetB, _ := decodeOffsetFetchResponse(t, bodyB)

	if offsetA != 111 {
		t.Errorf("group-a offset = %d, want 111", offsetA)
	}
	if offsetB != 222 {
		t.Errorf("group-b offset = %d, want 222", offsetB)
	}
}

// TestHandler_OffsetCommit_CorrelationID verifies the response echoes the request's correlationID.
func TestHandler_OffsetCommit_CorrelationID(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildOffsetCommitFrame(777, "c", "grp", "t", 0, 1))
	body := readResponse(t, clientConn)

	corrID, _ := decodeOffsetCommitResponse(t, body)
	if corrID != 777 {
		t.Errorf("correlationID = %d, want 777", corrID)
	}
}

// TestHandler_LoadOffsetsCache_HydratesFromDisk commits an offset via the
// handler, creates a new Handler backed by the same BrokerStore, calls
// LoadOffsetsCache, and verifies the offset is served correctly by
// handleOffsetFetch.
func TestHandler_LoadOffsetsCache_HydratesFromDisk(t *testing.T) {
	// First handler: commit an offset to disk.
	bs := newTestBrokerStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv1, cli1 := net.Pipe()
	h1 := NewHandler(logger, bs, metrics.New())
	go h1.Handle(srv1)

	cli1.Write(buildOffsetCommitFrame(1, "c", "my-group", "my-topic", 2, 77))
	body := readResponse(t, cli1)
	_, errCode := decodeOffsetCommitResponse(t, body)
	if errCode != 0 {
		t.Fatalf("commit error code = %d, want 0", errCode)
	}
	cli1.Close()

	// Second handler: fresh in-memory state, same on-disk store.
	h2 := NewHandler(logger, bs, metrics.New())
	if err := h2.LoadOffsetsCache(context.Background()); err != nil {
		t.Fatalf("LoadOffsetsCache: %v", err)
	}

	srv2, cli2 := net.Pipe()
	go h2.Handle(srv2)
	defer cli2.Close()

	cli2.Write(buildOffsetFetchFrame(2, "c", "my-group", "my-topic", 2))
	body = readResponse(t, cli2)

	_, offset, fetchErr := decodeOffsetFetchResponse(t, body)
	if fetchErr != 0 {
		t.Fatalf("fetch error code = %d, want 0", fetchErr)
	}
	if offset != 77 {
		t.Errorf("hydrated offset = %d, want 77", offset)
	}
}

// TestHandler_LoadOffsetsCache_FreshBroker verifies that LoadOffsetsCache on a
// store with no __consumer_offsets data returns nil without error.
func TestHandler_LoadOffsetsCache_FreshBroker(t *testing.T) {
	bs := newTestBrokerStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(logger, bs, metrics.New())

	if err := h.LoadOffsetsCache(context.Background()); err != nil {
		t.Fatalf("LoadOffsetsCache on fresh broker: %v", err)
	}
}

// TestHandler_OffsetCommit_WritesToConsumerOffsetsTopic verifies that
// __consumer_offsets exists in the BrokerStore after a commit.
func TestHandler_OffsetCommit_WritesToConsumerOffsetsTopic(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, bs := newTestHandler(t)
	go h.Handle(serverConn)

	clientConn.Write(buildOffsetCommitFrame(1, "c", "grp", "events", 0, 5))
	readResponse(t, clientConn)

	found := false
	for _, tp := range bs.Topics() {
		if tp.Topic == protocol.ConsumerOffsetsTopic {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %q topic to exist in BrokerStore after commit", protocol.ConsumerOffsetsTopic)
	}
}

// ── ListOffsets handler tests ──────────────────────────────────────────────────

func TestHandler_ListOffsets_Earliest(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	req := buildListOffsetsRequestFrame(55, "test", []protocol.ListOffsetsTopicRequest{
		{TopicName: "events", Partitions: []protocol.ListOffsetsPartitionRequest{
			{Partition: 0, Timestamp: protocol.TimestampEarliest},
		}},
	})
	clientConn.Write(req)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))

	corrID, _ := dec.ReadInt32()
	if corrID != 55 {
		t.Errorf("correlationID = %d, want 55", corrID)
	}
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition index
	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}
	dec.ReadInt64() // timestamp
	offset, _ := dec.ReadInt64()
	if offset != 0 {
		t.Errorf("earliest offset = %d, want 0", offset)
	}
}

func TestHandler_ListOffsets_Latest_Empty(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	req := buildListOffsetsRequestFrame(56, "test", []protocol.ListOffsetsTopicRequest{
		{TopicName: "events", Partitions: []protocol.ListOffsetsPartitionRequest{
			{Partition: 0, Timestamp: protocol.TimestampLatest},
		}},
	})
	clientConn.Write(req)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))

	dec.ReadInt32()  // correlationID
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition index
	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}
	dec.ReadInt64() // timestamp
	offset, _ := dec.ReadInt64()
	if offset != 0 {
		t.Errorf("latest offset on empty partition = %d, want 0", offset)
	}
}

func TestHandler_ListOffsets_Latest_AfterProduce(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Produce one message so the partition has non-zero size.
	produceFrame := buildProduceRequestFrame(1, "test", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "events", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("hello")},
		}},
	})
	clientConn.Write(produceFrame)
	readResponse(t, clientConn) // consume Produce response

	// Now query latest offset.
	req := buildListOffsetsRequestFrame(57, "test", []protocol.ListOffsetsTopicRequest{
		{TopicName: "events", Partitions: []protocol.ListOffsetsPartitionRequest{
			{Partition: 0, Timestamp: protocol.TimestampLatest},
		}},
	})
	clientConn.Write(req)

	body := readResponse(t, clientConn)
	dec := protocol.NewDecoder(bytes.NewReader(body))

	dec.ReadInt32()  // correlationID
	dec.ReadInt32()  // topic count
	dec.ReadString() // topic name
	dec.ReadInt32()  // partition count
	dec.ReadInt32()  // partition index
	errCode, _ := dec.ReadInt16()
	if errCode != 0 {
		t.Errorf("error code = %d, want 0", errCode)
	}
	dec.ReadInt64() // timestamp
	offset, _ := dec.ReadInt64()
	if offset <= 0 {
		t.Errorf("latest offset after produce = %d, want > 0", offset)
	}
}

// TestHandler_FetchRequest_BaseOffsetPatched verifies the physical-to-logical
// offset bridge. After producing two batches, fetching the second batch from
// its physical offset must return a RecordBatch whose BaseOffset satisfies:
//
//	BaseOffset + LastOffsetDelta + 1 == physicalOffsetOfNextBatch
//
// This is the exact formula KafkaJS uses to advance its fetch cursor, so
// getting this right prevents silent message discard ("duplicate" detection).
func TestHandler_FetchRequest_BaseOffsetPatched(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	h, _ := newTestHandler(t)
	go h.Handle(serverConn)

	// Produce the first batch and immediately fetch it so we can read the
	// HighWatermark, which equals the physical byte offset where the second
	// batch will start.
	frame1 := buildProduceRequestFrame(1, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("first-batch")},
		}},
	})
	clientConn.Write(frame1)
	readResponse(t, clientConn)

	fetchFrame1 := buildFetchRequestFrame(10, "c", 1<<20, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: 0, partitionMaxBytes: 1 << 20}},
	})
	clientConn.Write(fetchFrame1)
	body1 := readResponse(t, clientConn)
	_, _, topics1 := decodeFetchResponse(t, body1)
	if len(topics1) == 0 || len(topics1[0].partitions) == 0 {
		t.Fatal("no partitions in first fetch response")
	}
	// HWM after first batch = physical byte start of the second batch.
	secondBatchOffset := topics1[0].partitions[0].highWatermark

	// Produce the second batch.
	frame2 := buildProduceRequestFrame(2, "p", protocol.AcksLeader, []protocol.ProduceTopicData{
		{TopicName: "t", Partitions: []protocol.ProducePartitionData{
			{Partition: 0, RecordBatch: []byte("second-batch")},
		}},
	})
	clientConn.Write(frame2)
	readResponse(t, clientConn)

	// Fetch from the second batch's physical offset.
	fetchFrame2 := buildFetchRequestFrame(11, "c", 1<<20, map[string][]fetchPartition{
		"t": {{partition: 0, fetchOffset: secondBatchOffset, partitionMaxBytes: 1 << 20}},
	})
	clientConn.Write(fetchFrame2)
	body2 := readResponse(t, clientConn)
	_, _, topics2 := decodeFetchResponse(t, body2)
	if len(topics2) == 0 || len(topics2[0].partitions) == 0 {
		t.Fatal("no partitions in second fetch response")
	}
	batch := topics2[0].partitions[0].batch
	if len(batch) < 27 {
		t.Fatalf("second batch too short to parse: %d bytes", len(batch))
	}

	// Parse the patched RecordBatch header fields.
	baseOffset := int64(binary.BigEndian.Uint64(batch[0:8]))
	batchLen := int(binary.BigEndian.Uint32(batch[8:12]))
	lastOffsetDelta := int32(binary.BigEndian.Uint32(batch[23:27]))

	// KafkaJS next-cursor formula must point to the next physical batch start.
	// recHdr(4) is the on-disk length prefix that precedes the RecordBatch.
	const recHdr = 4
	nextCursor := baseOffset + int64(lastOffsetDelta) + 1
	expectedNext := secondBatchOffset + recHdr + int64(12+batchLen)

	if nextCursor != expectedNext {
		t.Errorf("next cursor = %d, want %d (BaseOffset=%d LastOffsetDelta=%d batchLen=%d)",
			nextCursor, expectedNext, baseOffset, lastOffsetDelta, batchLen)
	}
}
