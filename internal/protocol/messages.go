package protocol

import (
	"fmt"
	"io"
)

// API keys
const (
	ApiKeyProduce      int16 = 0
	ApiKeyFetch        int16 = 1
	ApiKeyMetadata     int16 = 3
	ApiKeyOffsetCommit int16 = 8
	ApiKeyOffsetFetch  int16 = 9
	ApiKeyApiVersions  int16 = 18
)

// ConsumerOffsetsTopic is the internal topic used to persist consumer group offsets.
const ConsumerOffsetsTopic = "__consumer_offsets"

// OffsetUnknown is returned by OffsetFetch when no offset has been committed yet.
const OffsetUnknown int64 = -1

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
	Replicas  []int32 // 24 bytes
	Isr       []int32 // 24 bytes
	Partition int32   // 4 bytes
	Leader    int32   // 4 bytes
	ErrorCode int16   // 2 bytes
}

type TopicMetadata struct {
	Partitions []PartitionMetadata // 24 bytes
	Name       string              // 16 bytes
	ErrorCode  int16               // 2 bytes
}

type Broker struct {
	Host   string // 16 bytes
	NodeID int32  // 4 bytes
	Port   int32  // 4 bytes
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
	RecordBatch []byte // 24 bytes — raw bytes of the Kafka RecordBatch
	Partition   int32  // 4 bytes
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
	Topics          []ProduceTopicData // 24 bytes
	TransactionalID string             // 16 bytes — empty when null
	Header          *RequestHeader     // 8 bytes
	TimeoutMs       int32              // 4 bytes
	Acks            int16              // 2 bytes
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
	BaseOffset int64 // 8 bytes — byte offset returned by storage.Segment.Append
	Partition  int32 // 4 bytes
	ErrorCode  int16 // 2 bytes
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

// ──────────────────────────────────────────────────────────────────────────────
// FetchRequest (ApiKey 1, v4)
// ──────────────────────────────────────────────────────────────────────────────

// FetchPartitionData holds the fetch parameters for one partition.
type FetchPartitionData struct {
	FetchOffset       int64 // 8 bytes — byte offset in the log to start reading from
	Partition         int32 // 4 bytes
	PartitionMaxBytes int32 // 4 bytes — max bytes to return for this partition
}

// FetchTopicData groups partition fetch requests under a single topic name.
type FetchTopicData struct {
	TopicName  string
	Partitions []FetchPartitionData
}

// FetchRequest (v4) wire layout after the request header:
//
//	ReplicaId      int32   (always -1 from consumers)
//	MaxWaitMs      int32
//	MinBytes       int32
//	MaxBytes       int32
//	IsolationLevel int8
//	topics[]
//	  TopicName           string
//	  partitions[]
//	    Partition         int32
//	    FetchOffset       int64
//	    PartitionMaxBytes int32
type FetchRequest struct {
	Topics         []FetchTopicData // 24 bytes
	Header         *RequestHeader   // 8 bytes
	ReplicaID      int32            // 4 bytes
	MaxWaitMs      int32            // 4 bytes
	MinBytes       int32            // 4 bytes
	MaxBytes       int32            // 4 bytes
	IsolationLevel int8             // 1 byte
}

// ParseFetchRequest reads a FetchRequest v4 body after the header.
func (d *Decoder) ParseFetchRequest(header *RequestHeader) (*FetchRequest, error) {
	replicaID, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("fetch request: read replica_id: %w", err)
	}

	maxWaitMs, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("fetch request: read max_wait_ms: %w", err)
	}

	minBytes, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("fetch request: read min_bytes: %w", err)
	}

	maxBytes, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("fetch request: read max_bytes: %w", err)
	}

	isolationLevel, err := d.ReadInt8()
	if err != nil {
		return nil, fmt.Errorf("fetch request: read isolation_level: %w", err)
	}

	topicCount, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("fetch request: read topic count: %w", err)
	}

	topics := make([]FetchTopicData, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		topicName, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("fetch request: topic[%d] name: %w", i, err)
		}

		partCount, err := d.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("fetch request: topic[%d] partition count: %w", i, err)
		}

		partitions := make([]FetchPartitionData, 0, partCount)
		for j := int32(0); j < partCount; j++ {
			partition, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("fetch request: topic[%d] partition[%d] index: %w", i, j, err)
			}
			fetchOffset, err := d.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("fetch request: topic[%d] partition[%d] fetch_offset: %w", i, j, err)
			}
			partMaxBytes, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("fetch request: topic[%d] partition[%d] partition_max_bytes: %w", i, j, err)
			}
			partitions = append(partitions, FetchPartitionData{
				Partition:         partition,
				FetchOffset:       fetchOffset,
				PartitionMaxBytes: partMaxBytes,
			})
		}
		topics = append(topics, FetchTopicData{TopicName: topicName, Partitions: partitions})
	}

	return &FetchRequest{
		Header:         header,
		ReplicaID:      replicaID,
		MaxWaitMs:      maxWaitMs,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		IsolationLevel: isolationLevel,
		Topics:         topics,
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// FetchResponse (ApiKey 1, v4)
// ──────────────────────────────────────────────────────────────────────────────

// Kafka error codes used in responses.
const (
	ErrCodeNone             int16 = 0
	ErrCodeOffsetOutOfRange int16 = 1
	ErrCodeCorruptMessage   int16 = 2
)

// FetchPartitionResponse holds the result for one fetched partition.
type FetchPartitionResponse struct {
	RecordBatch   io.Reader // reader over the raw record batch; nil when ErrorCode != 0
	HighWatermark int64     // 8 bytes — byte size of the log (end of log position)
	BatchSize     int32     // 4 bytes — byte length of RecordBatch; -1 when nil
	Partition     int32     // 4 bytes
	ErrorCode     int16     // 2 bytes
}

// FetchTopicResponse groups partition results under a topic name.
type FetchTopicResponse struct {
	TopicName  string
	Partitions []FetchPartitionResponse
}

// FetchResponse (v4) wire layout:
//
//	CorrelationID  int32
//	ThrottleTimeMs int32
//	topics[]
//	  TopicName    string
//	  partitions[]
//	    Partition      int32
//	    ErrorCode      int16
//	    HighWatermark  int64
//	    BatchSize      int32   (-1 when no records)
//	    RecordBatch    []byte  (absent when BatchSize == -1)
type FetchResponse struct {
	Topics         []FetchTopicResponse // 24 bytes
	ThrottleTimeMs int32                // 4 bytes
}

// WriteFetchResponse writes a complete length-prefixed FetchResponse frame to w.
//
// Strategy: pre-compute the total payload length (all sizes are known via
// BatchSize fields) so the 4-byte Kafka size prefix can be written first.
// Then, for each partition, write the fixed-size metadata fields and immediately
// stream the RecordBatch via io.Copy — so the wire order matches the Kafka
// protocol: [partition meta][batch bytes] interleaved per partition.
func WriteFetchResponse(w io.Writer, correlationID int32, resp *FetchResponse) error {
	// Pre-compute total payload size.
	// Per-partition fixed fields: partition(4) + errCode(2) + hwm(8) + batchSize(4) = 18 bytes.
	const partitionFixedBytes = 18
	// Frame header: corrID(4) + throttle(4) + topicCount(4) = 12 bytes.
	totalPayload := int32(12)
	for _, t := range resp.Topics {
		// topic: nameLen(2) + name + partCount(4).
		totalPayload += int32(2 + len(t.TopicName) + 4)
		for _, p := range t.Partitions {
			totalPayload += partitionFixedBytes
			if p.BatchSize > 0 {
				totalPayload += p.BatchSize
			}
		}
	}

	// Write 4-byte Kafka size prefix.
	var sizeBuf [4]byte
	sizeBuf[0] = byte(totalPayload >> 24)
	sizeBuf[1] = byte(totalPayload >> 16)
	sizeBuf[2] = byte(totalPayload >> 8)
	sizeBuf[3] = byte(totalPayload)
	if _, err := w.Write(sizeBuf[:]); err != nil {
		return err
	}

	// Write frame header.
	hdr := NewEncoder()
	hdr.WriteInt32(correlationID)
	hdr.WriteInt32(resp.ThrottleTimeMs)
	hdr.WriteInt32(int32(len(resp.Topics)))
	if _, err := w.Write(hdr.buffer.Bytes()); err != nil {
		return err
	}

	// Write each topic and its partitions, interleaving batch data immediately
	// after each partition's fixed metadata fields.
	for _, t := range resp.Topics {
		topicEnc := NewEncoder()
		topicEnc.WriteString(t.TopicName)
		topicEnc.WriteInt32(int32(len(t.Partitions)))
		if _, err := w.Write(topicEnc.buffer.Bytes()); err != nil {
			return err
		}

		for _, p := range t.Partitions {
			partEnc := NewEncoder()
			partEnc.WriteInt32(p.Partition)
			partEnc.WriteInt16(p.ErrorCode)
			partEnc.WriteInt64(p.HighWatermark)
			partEnc.WriteInt32(p.BatchSize)
			if _, err := w.Write(partEnc.buffer.Bytes()); err != nil {
				return err
			}
			if p.RecordBatch != nil && p.BatchSize > 0 {
				if _, err := io.Copy(w, p.RecordBatch); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// OffsetCommitRequest (ApiKey 8, v2)
// ──────────────────────────────────────────────────────────────────────────────

// OffsetCommitPartition holds the committed offset for one partition.
type OffsetCommitPartition struct {
	CommittedOffset   int64  // 8 bytes
	CommittedMetadata string // human-readable metadata (may be empty)
	Partition         int32  // 4 bytes
}

// OffsetCommitTopic groups partition commits under a topic.
type OffsetCommitTopic struct {
	TopicName  string
	Partitions []OffsetCommitPartition
}

// OffsetCommitRequest (v2) wire layout after the request header:
//
//	GroupID              string
//	GenerationID         int32
//	MemberID             string
//	RetentionTimeMs      int64
//	topics[]
//	  TopicName          string
//	  partitions[]
//	    Partition        int32
//	    CommittedOffset  int64
//	    CommittedMetadata string (nullable)
type OffsetCommitRequest struct {
	Topics          []OffsetCommitTopic
	GroupID         string
	MemberID        string
	RetentionTimeMs int64
	GenerationID    int32
}

// ParseOffsetCommitRequest reads an OffsetCommitRequest v2 body after the header.
func (d *Decoder) ParseOffsetCommitRequest(header *RequestHeader) (*OffsetCommitRequest, error) {
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("offset commit: read group_id: %w", err)
	}

	generationID, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("offset commit: read generation_id: %w", err)
	}

	memberID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("offset commit: read member_id: %w", err)
	}

	retentionTimeMs, err := d.ReadInt64()
	if err != nil {
		return nil, fmt.Errorf("offset commit: read retention_time_ms: %w", err)
	}

	topicCount, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("offset commit: read topic count: %w", err)
	}

	topics := make([]OffsetCommitTopic, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		topicName, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("offset commit: topic[%d] name: %w", i, err)
		}

		partCount, err := d.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("offset commit: topic[%d] partition count: %w", i, err)
		}

		partitions := make([]OffsetCommitPartition, 0, partCount)
		for j := int32(0); j < partCount; j++ {
			partition, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("offset commit: topic[%d] partition[%d] index: %w", i, j, err)
			}
			committedOffset, err := d.ReadInt64()
			if err != nil {
				return nil, fmt.Errorf("offset commit: topic[%d] partition[%d] offset: %w", i, j, err)
			}
			committedMetadata, err := d.ReadString()
			if err != nil {
				return nil, fmt.Errorf("offset commit: topic[%d] partition[%d] metadata: %w", i, j, err)
			}
			partitions = append(partitions, OffsetCommitPartition{
				Partition:         partition,
				CommittedOffset:   committedOffset,
				CommittedMetadata: committedMetadata,
			})
		}
		topics = append(topics, OffsetCommitTopic{TopicName: topicName, Partitions: partitions})
	}

	return &OffsetCommitRequest{
		GroupID:         groupID,
		GenerationID:    generationID,
		MemberID:        memberID,
		RetentionTimeMs: retentionTimeMs,
		Topics:          topics,
	}, nil
}

// OffsetCommitPartitionResponse is the result for one committed partition.
type OffsetCommitPartitionResponse struct {
	Partition int32
	ErrorCode int16
}

// OffsetCommitTopicResponse groups partition results under a topic.
type OffsetCommitTopicResponse struct {
	TopicName  string
	Partitions []OffsetCommitPartitionResponse
}

// OffsetCommitResponse (v2) wire layout:
//
//	CorrelationID  int32
//	topics[]
//	  TopicName    string
//	  partitions[]
//	    Partition  int32
//	    ErrorCode  int16
type OffsetCommitResponse struct {
	Topics []OffsetCommitTopicResponse
}

// EncodeOffsetCommitResponse serialises an OffsetCommitResponse into the Encoder.
func (e *Encoder) EncodeOffsetCommitResponse(correlationID int32, resp *OffsetCommitResponse) {
	e.WriteInt32(correlationID)
	e.WriteInt32(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.WriteString(t.TopicName)
		e.WriteInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.WriteInt32(p.Partition)
			e.WriteInt16(p.ErrorCode)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// OffsetFetchRequest (ApiKey 9, v1)
// ──────────────────────────────────────────────────────────────────────────────

// OffsetFetchPartition identifies a single partition to fetch an offset for.
type OffsetFetchPartition struct {
	Partition int32
}

// OffsetFetchTopic groups partitions under a topic.
type OffsetFetchTopic struct {
	TopicName  string
	Partitions []OffsetFetchPartition
}

// OffsetFetchRequest (v1) wire layout after the request header:
//
//	GroupID   string
//	topics[]
//	  TopicName  string
//	  partitions[]
//	    Partition int32
type OffsetFetchRequest struct {
	Topics  []OffsetFetchTopic
	GroupID string
}

// ParseOffsetFetchRequest reads an OffsetFetchRequest v1 body after the header.
func (d *Decoder) ParseOffsetFetchRequest(header *RequestHeader) (*OffsetFetchRequest, error) {
	groupID, err := d.ReadString()
	if err != nil {
		return nil, fmt.Errorf("offset fetch: read group_id: %w", err)
	}

	topicCount, err := d.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("offset fetch: read topic count: %w", err)
	}

	topics := make([]OffsetFetchTopic, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		topicName, err := d.ReadString()
		if err != nil {
			return nil, fmt.Errorf("offset fetch: topic[%d] name: %w", i, err)
		}

		partCount, err := d.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("offset fetch: topic[%d] partition count: %w", i, err)
		}

		partitions := make([]OffsetFetchPartition, 0, partCount)
		for j := int32(0); j < partCount; j++ {
			partition, err := d.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("offset fetch: topic[%d] partition[%d] index: %w", i, j, err)
			}
			partitions = append(partitions, OffsetFetchPartition{Partition: partition})
		}
		topics = append(topics, OffsetFetchTopic{TopicName: topicName, Partitions: partitions})
	}

	return &OffsetFetchRequest{GroupID: groupID, Topics: topics}, nil
}

// OffsetFetchPartitionResponse carries the committed offset for one partition.
type OffsetFetchPartitionResponse struct {
	CommittedOffset   int64  // OffsetUnknown (-1) when no commit exists
	CommittedMetadata string // empty when unknown
	Partition         int32
	ErrorCode         int16
}

// OffsetFetchTopicResponse groups partition results under a topic.
type OffsetFetchTopicResponse struct {
	TopicName  string
	Partitions []OffsetFetchPartitionResponse
}

// OffsetFetchResponse (v1) wire layout:
//
//	CorrelationID int32
//	topics[]
//	  TopicName   string
//	  partitions[]
//	    Partition          int32
//	    CommittedOffset    int64
//	    CommittedMetadata  string (nullable)
//	    ErrorCode          int16
type OffsetFetchResponse struct {
	Topics []OffsetFetchTopicResponse
}

// EncodeOffsetFetchResponse serialises an OffsetFetchResponse into the Encoder.
func (e *Encoder) EncodeOffsetFetchResponse(correlationID int32, resp *OffsetFetchResponse) {
	e.WriteInt32(correlationID)
	e.WriteInt32(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.WriteString(t.TopicName)
		e.WriteInt32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.WriteInt32(p.Partition)
			e.WriteInt64(p.CommittedOffset)
			e.WriteString(p.CommittedMetadata)
			e.WriteInt16(p.ErrorCode)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// ApiVersionsResponse (ApiKey 18, v0)
// ──────────────────────────────────────────────────────────────────────────────

// apiVersion describes a single supported API key with its version range.
type apiVersion struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}

// supportedAPIVersions is the static capability table advertised to clients.
var supportedAPIVersions = []apiVersion{
	{Key: ApiKeyProduce, MinVersion: 0, MaxVersion: 7},
	{Key: ApiKeyFetch, MinVersion: 0, MaxVersion: 11},
	{Key: ApiKeyMetadata, MinVersion: 0, MaxVersion: 9},
	{Key: ApiKeyApiVersions, MinVersion: 0, MaxVersion: 2},
}

// EncodeApiVersionsResponse serialises an ApiVersions response (v0) into the Encoder.
//
// Wire layout (after the 4-byte frame size prefix written by FullMessage):
//
//	CorrelationID  int32
//	ErrorCode      int16
//	api_versions[] int32 (array length)
//	  ApiKey       int16
//	  MinVersion   int16
//	  MaxVersion   int16
//	ThrottleTimeMs int32
func (e *Encoder) EncodeApiVersionsResponse(correlationID int32) {
	e.WriteInt32(correlationID)
	e.WriteInt16(ErrCodeNone)
	e.WriteInt32(int32(len(supportedAPIVersions)))
	for _, v := range supportedAPIVersions {
		e.WriteInt16(v.Key)
		e.WriteInt16(v.MinVersion)
		e.WriteInt16(v.MaxVersion)
	}
	e.WriteInt32(0) // ThrottleTimeMs
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
