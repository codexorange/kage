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

func TestEncodeMetadataResponse(t *testing.T) {
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
	enc.EncodeMetadataResponse(42, resp)
	raw := enc.FullMessage()

	if len(raw) < 4 {
		t.Fatalf("encoded response too short: %d bytes", len(raw))
	}

	// Parse the v6 wire layout.
	dec := NewDecoder(bytes.NewReader(raw))

	// 4-byte size prefix.
	size, _ := dec.ReadInt32()
	if size <= 0 {
		t.Fatalf("size prefix = %d, want > 0", size)
	}

	// CorrelationID.
	corrID, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("failed to read correlationID: %v", err)
	}
	if corrID != 42 {
		t.Errorf("correlationID = %d, want 42", corrID)
	}

	// ThrottleTimeMs (v1+).
	throttle, _ := dec.ReadInt32()
	if throttle != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", throttle)
	}

	// Broker count.
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
	// Rack: nullable string, -1 means null.
	rackLen, _ := dec.ReadInt16()
	if rackLen != -1 {
		t.Errorf("broker rack length = %d, want -1 (null)", rackLen)
	}

	// ClusterID: nullable string (v2+), -1 means null.
	clusterIDLen, _ := dec.ReadInt16()
	if clusterIDLen != -1 {
		t.Errorf("cluster_id length = %d, want -1 (null)", clusterIDLen)
	}

	// ControllerID (v1+).
	controllerID, _ := dec.ReadInt32()
	if controllerID != 1 {
		t.Errorf("controller_id = %d, want 1", controllerID)
	}

	// Topic count.
	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Errorf("topic count = %d, want 1", topicCount)
	}
	topicErr, _ := dec.ReadInt16()
	if topicErr != 0 {
		t.Errorf("topic error = %d, want 0", topicErr)
	}
	topicName, _ := dec.ReadString()
	if topicName != "kage-events" {
		t.Errorf("topic name = %q, want %q", topicName, "kage-events")
	}
	// IsInternal (v1+): int8.
	isInternal, _ := dec.ReadInt8()
	if isInternal != 0 {
		t.Errorf("is_internal = %d, want 0", isInternal)
	}
	// Partition count.
	partCount, _ := dec.ReadInt32()
	if partCount != 1 {
		t.Errorf("partition count = %d, want 1", partCount)
	}
	// Partition fields.
	partErr, _ := dec.ReadInt16()
	if partErr != 0 {
		t.Errorf("partition error = %d, want 0", partErr)
	}
	partIdx, _ := dec.ReadInt32()
	if partIdx != 0 {
		t.Errorf("partition index = %d, want 0", partIdx)
	}
	leaderID, _ := dec.ReadInt32()
	if leaderID != 1 {
		t.Errorf("leader_id = %d, want 1", leaderID)
	}
	replicaCount, _ := dec.ReadInt32()
	if replicaCount != 1 {
		t.Errorf("replica count = %d, want 1", replicaCount)
	}
	replicaNode, _ := dec.ReadInt32()
	if replicaNode != 1 {
		t.Errorf("replica node = %d, want 1", replicaNode)
	}
	isrCount, _ := dec.ReadInt32()
	if isrCount != 1 {
		t.Errorf("isr count = %d, want 1", isrCount)
	}
	isrNode, _ := dec.ReadInt32()
	if isrNode != 1 {
		t.Errorf("isr node = %d, want 1", isrNode)
	}
	offlineCount, _ := dec.ReadInt32()
	if offlineCount != 0 {
		t.Errorf("offline replicas count = %d, want 0", offlineCount)
	}
}
