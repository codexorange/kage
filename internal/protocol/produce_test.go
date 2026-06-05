package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// buildProduceRequestBody encodes a ProduceRequest v2 body (after the header).
// v2 layout: Acks(int16) | TimeoutMs(int32) | topics[]
// (No transactional_id — that field was introduced in v3.)
func buildProduceRequestBody(acks int16, timeoutMs int32, topics []ProduceTopicData) []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, acks)
	binary.Write(&buf, binary.BigEndian, timeoutMs)
	binary.Write(&buf, binary.BigEndian, int32(len(topics)))

	for _, t := range topics {
		binary.Write(&buf, binary.BigEndian, int16(len(t.TopicName)))
		buf.WriteString(t.TopicName)
		binary.Write(&buf, binary.BigEndian, int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			binary.Write(&buf, binary.BigEndian, p.Partition)
			binary.Write(&buf, binary.BigEndian, int32(len(p.RecordBatch)))
			buf.Write(p.RecordBatch)
		}
	}
	return buf.Bytes()
}

func TestParseProduceRequest_SingleTopicPartition(t *testing.T) {
	batch := []byte{0x01, 0x02, 0x03, 0x04} // opaque RecordBatch bytes

	topics := []ProduceTopicData{
		{
			TopicName: "kage-events",
			Partitions: []ProducePartitionData{
				{Partition: 0, RecordBatch: batch},
			},
		},
	}
	body := buildProduceRequestBody(AcksLeader, 5000, topics)
	hdr := &RequestHeader{ApiKey: ApiKeyProduce}

	req, err := NewDecoder(bytes.NewReader(body)).ParseProduceRequest(hdr)
	if err != nil {
		t.Fatalf("ParseProduceRequest: %v", err)
	}

	if req.Acks != AcksLeader {
		t.Errorf("Acks = %d, want %d", req.Acks, AcksLeader)
	}
	if req.TimeoutMs != 5000 {
		t.Errorf("TimeoutMs = %d, want 5000", req.TimeoutMs)
	}
	if len(req.Topics) != 1 {
		t.Fatalf("topics len = %d, want 1", len(req.Topics))
	}
	if req.Topics[0].TopicName != "kage-events" {
		t.Errorf("topic name = %q, want %q", req.Topics[0].TopicName, "kage-events")
	}
	if len(req.Topics[0].Partitions) != 1 {
		t.Fatalf("partitions len = %d, want 1", len(req.Topics[0].Partitions))
	}
	if req.Topics[0].Partitions[0].Partition != 0 {
		t.Errorf("partition = %d, want 0", req.Topics[0].Partitions[0].Partition)
	}
	if !bytes.Equal(req.Topics[0].Partitions[0].RecordBatch, batch) {
		t.Errorf("batch mismatch: got %v, want %v", req.Topics[0].Partitions[0].RecordBatch, batch)
	}
}

func TestParseProduceRequest_AcksNone(t *testing.T) {
	body := buildProduceRequestBody(AcksNone, 1000, []ProduceTopicData{
		{TopicName: "t", Partitions: []ProducePartitionData{{Partition: 0, RecordBatch: []byte("data")}}},
	})
	req, err := NewDecoder(bytes.NewReader(body)).ParseProduceRequest(&RequestHeader{})
	if err != nil {
		t.Fatalf("ParseProduceRequest: %v", err)
	}
	if req.Acks != AcksNone {
		t.Errorf("Acks = %d, want %d", req.Acks, AcksNone)
	}
}

func TestParseProduceRequest_AcksAll(t *testing.T) {
	body := buildProduceRequestBody(AcksAll, 1000, []ProduceTopicData{
		{TopicName: "t", Partitions: []ProducePartitionData{{Partition: 0, RecordBatch: []byte("x")}}},
	})
	req, err := NewDecoder(bytes.NewReader(body)).ParseProduceRequest(&RequestHeader{})
	if err != nil {
		t.Fatalf("ParseProduceRequest: %v", err)
	}
	if req.Acks != AcksAll {
		t.Errorf("Acks = %d, want %d", req.Acks, AcksAll)
	}
}

func TestParseProduceRequest_MultipleTopicsAndPartitions(t *testing.T) {
	topics := []ProduceTopicData{
		{
			TopicName: "topic-a",
			Partitions: []ProducePartitionData{
				{Partition: 0, RecordBatch: []byte("batch-a0")},
				{Partition: 1, RecordBatch: []byte("batch-a1")},
			},
		},
		{
			TopicName: "topic-b",
			Partitions: []ProducePartitionData{
				{Partition: 0, RecordBatch: []byte("batch-b0")},
			},
		},
	}
	body := buildProduceRequestBody(AcksLeader, 5000, topics)
	req, err := NewDecoder(bytes.NewReader(body)).ParseProduceRequest(&RequestHeader{})
	if err != nil {
		t.Fatalf("ParseProduceRequest: %v", err)
	}
	if len(req.Topics) != 2 {
		t.Fatalf("topics = %d, want 2", len(req.Topics))
	}
	if len(req.Topics[0].Partitions) != 2 {
		t.Errorf("topic-a partitions = %d, want 2", len(req.Topics[0].Partitions))
	}
	if len(req.Topics[1].Partitions) != 1 {
		t.Errorf("topic-b partitions = %d, want 1", len(req.Topics[1].Partitions))
	}
}

func TestParseProduceRequest_EmptyBatch(t *testing.T) {
	body := buildProduceRequestBody(AcksLeader, 1000, []ProduceTopicData{
		{TopicName: "t", Partitions: []ProducePartitionData{{Partition: 0, RecordBatch: []byte{}}}},
	})
	req, err := NewDecoder(bytes.NewReader(body)).ParseProduceRequest(&RequestHeader{})
	if err != nil {
		t.Fatalf("ParseProduceRequest with empty batch: %v", err)
	}
	if len(req.Topics[0].Partitions[0].RecordBatch) != 0 {
		t.Error("expected empty RecordBatch")
	}
}

func TestParseProduceRequest_Truncated(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{0x00}))
	_, err := d.ParseProduceRequest(&RequestHeader{})
	if err == nil {
		t.Fatal("expected error on truncated input, got nil")
	}
}

func TestEncodeProduceResponse(t *testing.T) {
	resp := &ProduceResponse{
		Topics: []ProduceTopicResponse{
			{
				TopicName: "kage-events",
				Partitions: []ProducePartitionResponse{
					{Partition: 0, ErrorCode: 0, BaseOffset: 1024},
				},
			},
		},
	}

	enc := NewEncoder()
	enc.EncodeProduceResponse(77, resp)
	raw := enc.FullMessage()

	if len(raw) < 4 {
		t.Fatalf("encoded response too short: %d bytes", len(raw))
	}

	dec := NewDecoder(bytes.NewReader(raw))
	size, _ := dec.ReadInt32()
	if size <= 0 {
		t.Fatalf("size prefix = %d, want > 0", size)
	}

	corrID, _ := dec.ReadInt32()
	if corrID != 77 {
		t.Errorf("correlationID = %d, want 77", corrID)
	}

	topicCount, _ := dec.ReadInt32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}

	topicName, _ := dec.ReadString()
	if topicName != "kage-events" {
		t.Errorf("topic name = %q, want %q", topicName, "kage-events")
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
	if baseOffset != 1024 {
		t.Errorf("base offset = %d, want 1024", baseOffset)
	}
}

func TestEncodeProduceResponse_MultipleTopics(t *testing.T) {
	resp := &ProduceResponse{
		Topics: []ProduceTopicResponse{
			{TopicName: "a", Partitions: []ProducePartitionResponse{{Partition: 0, ErrorCode: 0, BaseOffset: 0}}},
			{TopicName: "b", Partitions: []ProducePartitionResponse{{Partition: 0, ErrorCode: 0, BaseOffset: 42}}},
		},
	}
	enc := NewEncoder()
	enc.EncodeProduceResponse(1, resp)
	raw := enc.FullMessage()

	dec := NewDecoder(bytes.NewReader(raw))
	dec.ReadInt32() // size prefix
	dec.ReadInt32() // correlationID

	count, _ := dec.ReadInt32()
	if count != 2 {
		t.Errorf("topic count = %d, want 2", count)
	}
}
