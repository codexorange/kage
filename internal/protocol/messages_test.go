package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func buildRequestHeaderBytes(size int32, apiKey, apiVersion int16, correlationID int32, clientID string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, size)
	binary.Write(&buf, binary.BigEndian, apiKey)
	binary.Write(&buf, binary.BigEndian, apiVersion)
	binary.Write(&buf, binary.BigEndian, correlationID)
	binary.Write(&buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)
	return buf.Bytes()
}

func TestParseRequestHeader(t *testing.T) {
	tests := []struct {
		name          string
		size          int32
		apiKey        int16
		apiVersion    int16
		correlationID int32
		clientID      string
	}{
		{"api versions request", 14, 18, 3, 1, "test-client"},
		{"zero values", 0, 0, 0, 0, ""},
		{"negative correlation id", 100, 1, 0, -1, "producer-1"},
		{"large size", 65535, 18, 4, 999, "consumer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := buildRequestHeaderBytes(tt.size, tt.apiKey, tt.apiVersion, tt.correlationID, tt.clientID)
			d := NewDecoder(bytes.NewReader(data))
			hdr, err := d.ParseRequestHeader()
			if err != nil {
				t.Fatalf("ParseRequestHeader() unexpected error: %v", err)
			}
			if hdr.Size != tt.size {
				t.Errorf("Size = %d, want %d", hdr.Size, tt.size)
			}
			if hdr.ApiKey != tt.apiKey {
				t.Errorf("ApiKey = %d, want %d", hdr.ApiKey, tt.apiKey)
			}
			if hdr.ApiVersion != tt.apiVersion {
				t.Errorf("ApiVersion = %d, want %d", hdr.ApiVersion, tt.apiVersion)
			}
			if hdr.CorrelationID != tt.correlationID {
				t.Errorf("CorrelationID = %d, want %d", hdr.CorrelationID, tt.correlationID)
			}
			if hdr.ClientID != tt.clientID {
				t.Errorf("ClientID = %q, want %q", hdr.ClientID, tt.clientID)
			}
		})
	}
}

func TestParseRequestHeader_Truncated(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
	}{
		{"empty", []byte{}},
		{"only 2 bytes", []byte{0x00, 0x00}},
		{"missing api version and correlation id", func() []byte {
			var buf bytes.Buffer
			binary.Write(&buf, binary.BigEndian, int32(10))
			binary.Write(&buf, binary.BigEndian, int16(18))
			return buf.Bytes()
		}()},
		{"missing correlation id", func() []byte {
			var buf bytes.Buffer
			binary.Write(&buf, binary.BigEndian, int32(10))
			binary.Write(&buf, binary.BigEndian, int16(18))
			binary.Write(&buf, binary.BigEndian, int16(3))
			return buf.Bytes()
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.bytes))
			_, err := d.ParseRequestHeader()
			if err == nil {
				t.Fatal("expected error on truncated input, got nil")
			}
		})
	}
}

func TestParseRequestHeader_EOF(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{}))
	_, err := d.ParseRequestHeader()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestRequestHeaderStruct(t *testing.T) {
	hdr := &RequestHeader{
		Size:          512,
		ApiKey:        18,
		ApiVersion:    4,
		CorrelationID: 77,
	}
	if hdr.Size != 512 {
		t.Errorf("Size mismatch")
	}
	if hdr.ApiKey != 18 {
		t.Errorf("ApiKey mismatch")
	}
	if hdr.ApiVersion != 4 {
		t.Errorf("ApiVersion mismatch")
	}
	if hdr.CorrelationID != 77 {
		t.Errorf("CorrelationID mismatch")
	}
}

func TestResponseHeaderStruct(t *testing.T) {
	hdr := &ResponseHeader{CorrelationID: 42}
	if hdr.CorrelationID != 42 {
		t.Errorf("CorrelationID = %d, want 42", hdr.CorrelationID)
	}
}

// buildMetadataRequestBytes encodes a MetadataRequest body (topic list only).
func buildMetadataRequestBytes(topics []string) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&buf, binary.BigEndian, int16(len(t)))
		buf.WriteString(t)
	}
	return buf.Bytes()
}

func TestParseMetadataRequest(t *testing.T) {
	tests := []struct {
		name   string
		topics []string
	}{
		{"single topic", []string{"kage-events"}},
		{"multiple topics", []string{"topic-a", "topic-b", "topic-c"}},
		{"empty list (fetch all)", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := buildMetadataRequestBytes(tt.topics)
			d := NewDecoder(bytes.NewReader(data))
			hdr := &RequestHeader{ApiKey: ApiKeyMetadata}
			req, err := d.ParseMetadataRequest(hdr)
			if err != nil {
				t.Fatalf("ParseMetadataRequest() unexpected error: %v", err)
			}
			if len(req.Topics) != len(tt.topics) {
				t.Fatalf("topics len = %d, want %d", len(req.Topics), len(tt.topics))
			}
			for i, topic := range tt.topics {
				if req.Topics[i] != topic {
					t.Errorf("topic[%d] = %q, want %q", i, req.Topics[i], topic)
				}
			}
		})
	}
}

func TestParseMetadataRequest_Truncated(t *testing.T) {
	// Only partial array length bytes — must fail.
	d := NewDecoder(bytes.NewReader([]byte{0x00}))
	_, err := d.ParseMetadataRequest(&RequestHeader{})
	if err == nil {
		t.Fatal("expected error on truncated input, got nil")
	}
}

func TestEncodeMetadataResponse_V6(t *testing.T) {
	resp := &MetadataResponse{
		Brokers:      []Broker{{NodeID: 1, Host: "localhost", Port: 9092, Rack: nil}},
		ClusterID:    nil,
		ControllerID: 1,
		Topics: []TopicMetadata{
			{
				ErrorCode:  0,
				Name:       "kage-events",
				IsInternal: false,
				Partitions: []PartitionMetadata{
					{ErrorCode: 0, Partition: 0, Leader: 1, Replicas: []int32{1}, Isr: []int32{1}, OfflineReplicas: nil},
				},
			},
		},
	}

	enc := NewEncoder()
	enc.EncodeMetadataResponse(42, 6, resp)
	raw := enc.FullMessage()

	dec := NewDecoder(bytes.NewReader(raw))
	dec.ReadInt32() // size prefix

	corrID, _ := dec.ReadInt32()
	if corrID != 42 {
		t.Errorf("correlationID = %d, want 42", corrID)
	}
	throttle, _ := dec.ReadInt32() // ThrottleTimeMs (v3+)
	if throttle != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", throttle)
	}
	brokerCount, _ := dec.ReadInt32()
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1", brokerCount)
	}
	nodeID, _ := dec.ReadInt32()
	if nodeID != 1 {
		t.Errorf("broker NodeID = %d, want 1", nodeID)
	}
	host, _ := dec.ReadString()
	if host != "localhost" {
		t.Errorf("broker host = %q, want %q", host, "localhost")
	}
	port, _ := dec.ReadInt32()
	if port != 9092 {
		t.Errorf("broker port = %d, want 9092", port)
	}
	rackLen, _ := dec.ReadInt16() // Rack nullable (v1+)
	if rackLen != -1 {
		t.Errorf("broker rack length = %d, want -1 (null)", rackLen)
	}
	clusterIDLen, _ := dec.ReadInt16() // ClusterID nullable (v2+)
	if clusterIDLen != -1 {
		t.Errorf("cluster_id length = %d, want -1 (null)", clusterIDLen)
	}
	controllerID, _ := dec.ReadInt32() // ControllerID (v1+)
	if controllerID != 1 {
		t.Errorf("controller_id = %d, want 1", controllerID)
	}
	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Errorf("topic count = %d, want 1", topicCount)
	}
	dec.ReadInt16() // topic error code
	topicName, _ := dec.ReadString()
	if topicName != "kage-events" {
		t.Errorf("topic name = %q, want %q", topicName, "kage-events")
	}
	isInternal, _ := dec.ReadInt8() // IsInternal (v1+)
	if isInternal != 0 {
		t.Errorf("is_internal = %d, want 0", isInternal)
	}
	partCount, _ := dec.ReadInt32()
	if partCount != 1 {
		t.Errorf("partition count = %d, want 1", partCount)
	}
	dec.ReadInt16() // partition error
	dec.ReadInt32() // partition index
	leaderID, _ := dec.ReadInt32()
	if leaderID != 1 {
		t.Errorf("leader_id = %d, want 1", leaderID)
	}
	dec.ReadInt32() // replica count
	dec.ReadInt32() // replica node
	dec.ReadInt32() // isr count
	dec.ReadInt32() // isr node
	offlineCount, _ := dec.ReadInt32() // offlineReplicas (v5+)
	if offlineCount != 0 {
		t.Errorf("offline replicas count = %d, want 0", offlineCount)
	}
}

func TestEncodeMetadataResponse_V0(t *testing.T) {
	// v0 layout: no ThrottleTimeMs, no Rack, no ClusterID, no ControllerID,
	// no IsInternal, no OfflineReplicas.
	resp := &MetadataResponse{
		Brokers:      []Broker{{NodeID: 7, Host: "kage", Port: 9092, Rack: nil}},
		ClusterID:    nil,
		ControllerID: 7,
		Topics: []TopicMetadata{
			{
				Name:      "t",
				ErrorCode: 0,
				Partitions: []PartitionMetadata{
					{ErrorCode: 0, Partition: 0, Leader: 7, Replicas: []int32{7}, Isr: []int32{7}},
				},
			},
		},
	}

	enc := NewEncoder()
	enc.EncodeMetadataResponse(1, 0, resp)
	raw := enc.FullMessage()

	dec := NewDecoder(bytes.NewReader(raw))
	dec.ReadInt32() // size prefix

	corrID, _ := dec.ReadInt32()
	if corrID != 1 {
		t.Errorf("correlationID = %d, want 1", corrID)
	}
	// No ThrottleTimeMs in v0 — next field is broker count.
	brokerCount, _ := dec.ReadInt32()
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1", brokerCount)
	}
	nodeID, _ := dec.ReadInt32()
	if nodeID != 7 {
		t.Errorf("broker NodeID = %d, want 7", nodeID)
	}
	dec.ReadString() // host
	dec.ReadInt32()  // port
	// No Rack in v0 — next field is topic count (no ClusterID/ControllerID either).
	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Errorf("topic count = %d, want 1", topicCount)
	}
	dec.ReadInt16() // topic error
	name, _ := dec.ReadString()
	if name != "t" {
		t.Errorf("topic name = %q, want %q", name, "t")
	}
	// No IsInternal in v0 — next field is partition count.
	partCount, _ := dec.ReadInt32()
	if partCount != 1 {
		t.Errorf("partition count = %d, want 1", partCount)
	}
	dec.ReadInt16() // partition error
	dec.ReadInt32() // partition index
	dec.ReadInt32() // leader
	dec.ReadInt32() // replica count
	dec.ReadInt32() // replica node
	isrCount, _ := dec.ReadInt32()
	if isrCount != 1 {
		t.Errorf("isr count = %d, want 1", isrCount)
	}
	// No OfflineReplicas in v0 — stream ends here.
}

// ── ListOffsets protocol tests ─────────────────────────────────────────────────

func buildListOffsetsRequestBody(replicaID int32, topics []ListOffsetsTopicRequest) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, replicaID)
	binary.Write(&buf, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&buf, binary.BigEndian, int16(len(t.TopicName)))
		buf.WriteString(t.TopicName)
		binary.Write(&buf, binary.BigEndian, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			binary.Write(&buf, binary.BigEndian, p.Partition)
			binary.Write(&buf, binary.BigEndian, p.Timestamp)
		}
	}
	return buf.Bytes()
}

func TestParseListOffsetsRequest_Earliest(t *testing.T) {
	topics := []ListOffsetsTopicRequest{
		{TopicName: "events", Partitions: []ListOffsetsPartitionRequest{{Partition: 0, Timestamp: TimestampEarliest}}},
	}
	body := buildListOffsetsRequestBody(-1, topics)
	req, err := NewDecoder(bytes.NewReader(body)).ParseListOffsetsRequest(&RequestHeader{ApiVersion: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.ReplicaID != -1 {
		t.Errorf("ReplicaID = %d, want -1", req.ReplicaID)
	}
	if len(req.Topics) != 1 {
		t.Fatalf("topics = %d, want 1", len(req.Topics))
	}
	if req.Topics[0].TopicName != "events" {
		t.Errorf("topic name = %q, want %q", req.Topics[0].TopicName, "events")
	}
	if req.Topics[0].Partitions[0].Timestamp != TimestampEarliest {
		t.Errorf("timestamp = %d, want %d", req.Topics[0].Partitions[0].Timestamp, TimestampEarliest)
	}
}

func TestParseListOffsetsRequest_Latest(t *testing.T) {
	topics := []ListOffsetsTopicRequest{
		{TopicName: "logs", Partitions: []ListOffsetsPartitionRequest{{Partition: 2, Timestamp: TimestampLatest}}},
	}
	body := buildListOffsetsRequestBody(-1, topics)
	req, err := NewDecoder(bytes.NewReader(body)).ParseListOffsetsRequest(&RequestHeader{ApiVersion: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Topics[0].Partitions[0].Timestamp != TimestampLatest {
		t.Errorf("timestamp = %d, want %d", req.Topics[0].Partitions[0].Timestamp, TimestampLatest)
	}
	if req.Topics[0].Partitions[0].Partition != 2 {
		t.Errorf("partition = %d, want 2", req.Topics[0].Partitions[0].Partition)
	}
}

func TestEncodeListOffsetsResponse(t *testing.T) {
	resp := &ListOffsetsResponse{
		Topics: []ListOffsetsTopicResponse{
			{
				TopicName: "events",
				Partitions: []ListOffsetsPartitionResponse{
					{Partition: 0, ErrorCode: 0, Timestamp: -1, Offset: 42},
				},
			},
		},
	}
	enc := NewEncoder()
	enc.EncodeListOffsetsResponse(77, resp)
	raw := enc.FullMessage()

	dec := NewDecoder(bytes.NewReader(raw))
	dec.ReadInt32() // size prefix

	corrID, _ := dec.ReadInt32()
	if corrID != 77 {
		t.Errorf("correlationID = %d, want 77", corrID)
	}
	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}
	topicName, _ := dec.ReadString()
	if topicName != "events" {
		t.Errorf("topic name = %q, want %q", topicName, "events")
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
	ts, _ := dec.ReadInt64()
	if ts != -1 {
		t.Errorf("timestamp = %d, want -1", ts)
	}
	offset, _ := dec.ReadInt64()
	if offset != 42 {
		t.Errorf("offset = %d, want 42", offset)
	}
}
