package protocol

import "fmt"

// API keys
const (
	ApiKeyProduce  int16 = 0
	ApiKeyMetadata int16 = 3
)

// Acks values for ProduceRequest.
const (
	AcksNone   int16 = 0  // fire-and-forget — no response sent
	AcksLeader int16 = 1  // respond after leader write
	AcksAll    int16 = -1 // respond after all in-sync replicas write
)

type RequestHeader struct {
	Size          int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
}

type ResponseHeader struct {
	CorrelationID int32
}

// MetadataRequest (ApiKey 3)
type MetadataRequest struct {
	Header *RequestHeader
	Topics []string
}

// MetadataResponse (ApiKey 3)
type PartitionMetadata struct {
	ErrorCode int16
	Partition int32
	Leader    int32
	Replicas  []int32
	Isr       []int32
}

type TopicMetadata struct {
	ErrorCode  int16
	Name       string
	Partitions []PartitionMetadata
}

type Broker struct {
	NodeID int32
	Host   string
	Port   int32
}

type MetadataResponse struct {
	Brokers []Broker
	Topics  []TopicMetadata
}

func (d *Decoder) ParseRequestHeader() (*RequestHeader, error) {
	size, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}

	apiKey, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}

	apiVersion, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}

	correlationID, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}

	clientID, err := d.ReadString()
	if err != nil {
		return nil, err
	}

	return &RequestHeader{
		Size:          size,
		ApiKey:        apiKey,
		ApiVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, nil
}

// ParseMetadataRequest reads a MetadataRequest body after the header has been parsed.
// Format: [topics_array_len int32] ([topic_name string] ...)
func (d *Decoder) ParseMetadataRequest(header *RequestHeader) (*MetadataRequest, error) {
	count, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("metadata request: failed to read topics array length: %w", err)
	}

	topics := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		name, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("metadata request: failed to read topic name at index %d: %w", i, err)
		}
		topics = append(topics, name)
	}

	return &MetadataRequest{Header: header, Topics: topics}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// ProduceRequest (ApiKey 0, v3)
// ──────────────────────────────────────────────────────────────────────────────

// ProducePartitionData holds the raw RecordBatch bytes for one partition.
// We treat the batch as opaque bytes — Kage does not parse individual records.
type ProducePartitionData struct {
	Partition   int32
	RecordBatch []byte // raw bytes of the Kafka RecordBatch
}

// ProduceTopicData groups partition batches under a single topic name.
type ProduceTopicData struct {
	TopicName  string
	Partitions []ProducePartitionData
}

// ProduceRequest (v3) wire layout after the request header:
//
//	TransactionalID  string (nullable: int16=-1 means null)
//	Acks             int16
//	TimeoutMs        int32
//	topics[]
//	  TopicName      string
//	  partitions[]
//	    Partition    int32
//	    BatchSize    int32   (byte length of RecordBatch)
//	    RecordBatch  []byte
type ProduceRequest struct {
	Header          *RequestHeader
	TransactionalID string // empty when null
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceTopicData
}

// ParseProduceRequest reads the ProduceRequest body after the header.
func (d *Decoder) ParseProduceRequest(header *RequestHeader) (*ProduceRequest, error) {
	// TransactionalID is a nullable string: length int16, -1 means null/absent.
	txnIDLen, err := d.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("produce request: read transactional_id length: %w", err)
	}
	var txnID string
	if txnIDLen > 0 {
		raw, err := d.ReadBytes(int(txnIDLen))
		if err != nil {
			return nil, fmt.Errorf("produce request: read transactional_id: %w", err)
		}
		txnID = string(raw)
	}

	acks, err := d.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("produce request: read acks: %w", err)
	}

	timeoutMs, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("produce request: read timeout_ms: %w", err)
	}

	topicCount, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("produce request: read topic count: %w", err)
	}

	topics := make([]ProduceTopicData, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		topicName, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("produce request: topic[%d] name: %w", i, err)
		}

		partCount, err := d.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("produce request: topic[%d] partition count: %w", i, err)
		}

		partitions := make([]ProducePartitionData, 0, partCount)
		for j := int32(0); j < partCount; j++ {
			partition, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("produce request: topic[%d] partition[%d] index: %w", i, j, err)
			}

			batchSize, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("produce request: topic[%d] partition[%d] batch size: %w", i, j, err)
			}
			if batchSize < 0 {
				return nil, fmt.Errorf("produce request: topic[%d] partition[%d] negative batch size %d", i, j, batchSize)
			}

			batch, err := d.ReadBytes(int(batchSize))
			if err != nil {
				return nil, fmt.Errorf("produce request: topic[%d] partition[%d] batch: %w", i, j, err)
			}

			partitions = append(partitions, ProducePartitionData{
				Partition:   partition,
				RecordBatch: batch,
			})
		}
		topics = append(topics, ProduceTopicData{TopicName: topicName, Partitions: partitions})
	}

	return &ProduceRequest{
		Header:          header,
		TransactionalID: txnID,
		Acks:            acks,
		TimeoutMs:       timeoutMs,
		Topics:          topics,
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// ProduceResponse (ApiKey 0, v0)
// ──────────────────────────────────────────────────────────────────────────────

// ProducePartitionResponse carries the result for a single partition write.
type ProducePartitionResponse struct {
	Partition  int32
	ErrorCode  int16
	BaseOffset int64 // byte offset returned by storage.Segment.Append
}

// ProduceTopicResponse groups partition results under a topic name.
type ProduceTopicResponse struct {
	TopicName  string
	Partitions []ProducePartitionResponse
}

// ProduceResponse (v0) wire layout:
//
//	CorrelationID int32
//	topics[]
//	  TopicName   string
//	  partitions[]
//	    Partition  int32
//	    ErrorCode  int16
//	    BaseOffset int64
type ProduceResponse struct {
	Topics []ProduceTopicResponse
}

// EncodeProduceResponse serialises a ProduceResponse into the Encoder.
func (e *Encoder) EncodeProduceResponse(correlationID int32, resp *ProduceResponse) {
	e.WriteInt32(correlationID)

	e.WriteInt32(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.WriteString(t.TopicName)
		e.WriteInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.WriteInt32(p.Partition)
			e.WriteInt16(p.ErrorCode)
			e.WriteInt64(p.BaseOffset)
		}
	}
}

// EncodeMetadataResponse encodes a MetadataResponse into the Encoder.
// Wire layout (v0):
//
//	CorrelationID int32
//	brokers_array_len int32
//	  [NodeID int32, Host string, Port int32] ...
//	topics_array_len int32
//	  [ErrorCode int16, Name string, partitions_array_len int32
//	    [ErrorCode int16, Partition int32, Leader int32,
//	     replicas_array_len int32, [replica int32]...,
//	     isr_array_len    int32, [isr int32]...]...]
func (e *Encoder) EncodeMetadataResponse(correlationID int32, resp *MetadataResponse) {
	e.WriteInt32(correlationID)

	e.WriteInt32(int32(len(resp.Brokers)))
	for _, b := range resp.Brokers {
		e.WriteInt32(b.NodeID)
		e.WriteString(b.Host)
		e.WriteInt32(b.Port)
	}

	e.WriteInt32(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.WriteInt16(t.ErrorCode)
		e.WriteString(t.Name)
		e.WriteInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.WriteInt16(p.ErrorCode)
			e.WriteInt32(p.Partition)
			e.WriteInt32(p.Leader)
			e.WriteInt32(int32(len(p.Replicas)))
			for _, r := range p.Replicas {
				e.WriteInt32(r)
			}
			e.WriteInt32(int32(len(p.Isr)))
			for _, r := range p.Isr {
				e.WriteInt32(r)
			}
		}
	}
}
