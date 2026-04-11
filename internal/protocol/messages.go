package protocol

import "fmt"

// API keys
const (
	ApiKeyMetadata int16 = 3
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
