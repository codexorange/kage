package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/codexorange/kage/internal/metrics"
	"github.com/codexorange/kage/internal/protocol"
	"github.com/codexorange/kage/internal/storage"
)

// offsetCacheKey uniquely identifies a consumer group's committed offset for
// a single topic-partition.
type offsetCacheKey struct {
	GroupID   string
	Topic     string
	Partition int32
}

// Handler routes incoming Kafka requests to the appropriate response builder.
type Handler struct {
	offsetMu    sync.RWMutex
	offsetCache map[offsetCacheKey]int64
	logger      *slog.Logger
	store       *storage.BrokerStore
	metrics     *metrics.Metrics
}

func NewHandler(logger *slog.Logger, store *storage.BrokerStore, m *metrics.Metrics) *Handler {
	return &Handler{
		offsetCache: make(map[offsetCacheKey]int64),
		logger:      logger,
		store:       store,
		metrics:     m,
	}
}

// Handle reads requests from conn in a loop, dispatching each by ApiKey, until
// the connection is closed or an unrecoverable error occurs.
func (h *Handler) Handle(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	h.logger.Debug("connection established", "client", remote)

	decoder := protocol.NewDecoder(conn)

	for {
		header, err := decoder.ParseRequestHeader()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				h.logger.Error("failed to parse request header", "client", remote, "error", err)
			}
			return
		}

		h.logger.Info("request received",
			"client", remote,
			"api_key", header.ApiKey,
			"api_version", header.ApiVersion,
			"correlation_id", header.CorrelationID,
			"client_id", header.ClientID,
		)

		if err := h.dispatch(conn, decoder, header); err != nil {
			h.logger.Error("failed to handle request",
				"client", remote,
				"api_key", header.ApiKey,
				"error", err,
			)
			return
		}
	}
}

func (h *Handler) dispatch(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	switch header.ApiKey {
	case protocol.ApiKeyProduce:
		return h.handleProduce(conn, dec, header)
	case protocol.ApiKeyFetch:
		return h.handleFetch(conn, dec, header)
	case protocol.ApiKeyMetadata:
		return h.handleMetadata(conn, dec, header)
	case protocol.ApiKeyOffsetCommit:
		return h.handleOffsetCommit(conn, dec, header)
	case protocol.ApiKeyOffsetFetch:
		return h.handleOffsetFetch(conn, dec, header)
	default:
		return fmt.Errorf("unsupported api_key: %d", header.ApiKey)
	}
}

// handleProduce processes a ProduceRequest (ApiKey 0).
//
// Acks semantics:
//   - acks=0  (AcksNone)   — write to storage, send NO response.
//   - acks=1  (AcksLeader) — write to storage, respond with offset.
//   - acks=-1 (AcksAll)    — treated as acks=1 (no replication yet).
func (h *Handler) handleProduce(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	req, err := dec.ParseProduceRequest(header)
	if err != nil {
		return fmt.Errorf("handleProduce: parse: %w", err)
	}

	resp := &protocol.ProduceResponse{
		Topics: make([]protocol.ProduceTopicResponse, 0, len(req.Topics)),
	}

	for _, topic := range req.Topics {
		topicResp := protocol.ProduceTopicResponse{
			TopicName:  topic.TopicName,
			Partitions: make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions)),
		}

		for _, part := range topic.Partitions {
			partResp := protocol.ProducePartitionResponse{
				Partition: part.Partition,
			}

			// Validate RecordBatch integrity (magic byte + CRC32C).
			// Skip validation for empty batches (no records to validate).
			var recordCount int32
			if len(part.RecordBatch) > 0 {
				rc, err := protocol.ValidateRecordBatch(part.RecordBatch)
				if err != nil {
					h.logger.Warn("produce: invalid record batch",
						"topic", topic.TopicName,
						"partition", part.Partition,
						"error", err,
					)
					partResp.ErrorCode = protocol.ErrCodeCorruptMessage
					partResp.BaseOffset = -1
					topicResp.Partitions = append(topicResp.Partitions, partResp)
					continue
				}
				recordCount = rc
			}

			ps, err := h.store.GetOrCreatePartition(topic.TopicName, part.Partition)
			if err != nil {
				h.logger.Error("produce: failed to get partition store",
					"topic", topic.TopicName,
					"partition", part.Partition,
					"error", err,
				)
				partResp.ErrorCode = 1
				partResp.BaseOffset = -1
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			start := time.Now()
			offset, err := ps.Append(part.RecordBatch)
			h.metrics.ObserveDiskWrite(topic.TopicName, start)

			if err != nil {
				h.logger.Error("produce: storage append failed",
					"topic", topic.TopicName,
					"partition", part.Partition,
					"error", err,
				)
				partResp.ErrorCode = 1
				partResp.BaseOffset = -1
			} else {
				partResp.ErrorCode = 0
				partResp.BaseOffset = int64(offset)
				// Count individual records, not batches.
				h.metrics.MessagesProducedTotal.WithLabelValues(topic.TopicName).Add(float64(recordCount))
				h.logger.Info("produce: batch stored",
					"topic", topic.TopicName,
					"partition", part.Partition,
					"offset", offset,
					"batch_bytes", len(part.RecordBatch),
					"record_count", recordCount,
				)
			}
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	// acks=0: the producer does not expect a response — do not write anything.
	if req.Acks == protocol.AcksNone {
		return nil
	}

	enc := protocol.NewEncoder()
	enc.EncodeProduceResponse(header.CorrelationID, resp)
	if _, err := conn.Write(enc.FullMessage()); err != nil {
		return fmt.Errorf("handleProduce: write response: %w", err)
	}
	return nil
}

// handleFetch processes a FetchRequest (ApiKey 1, v4).
//
// For each topic-partition requested:
//   - Clamp the effective max bytes to min(PartitionMaxBytes, remaining global MaxBytes).
//   - Read bytes from storage starting at FetchOffset.
//   - On ErrInvalidOffset, respond with ErrCodeOffsetOutOfRange.
//   - Decrement the global bytes budget after each successful read.
func (h *Handler) handleFetch(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	req, err := dec.ParseFetchRequest(header)
	if err != nil {
		return fmt.Errorf("handleFetch: parse: %w", err)
	}

	remaining := req.MaxBytes

	resp := &protocol.FetchResponse{
		Topics: make([]protocol.FetchTopicResponse, 0, len(req.Topics)),
	}

	for _, topic := range req.Topics {
		topicResp := protocol.FetchTopicResponse{
			TopicName:  topic.TopicName,
			Partitions: make([]protocol.FetchPartitionResponse, 0, len(topic.Partitions)),
		}

		for _, part := range topic.Partitions {
			partResp := protocol.FetchPartitionResponse{
				Partition: part.Partition,
				BatchSize: -1,
			}

			ps, err := h.store.GetOrCreatePartition(topic.TopicName, part.Partition)
			if err != nil {
				h.logger.Error("fetch: failed to get partition store",
					"topic", topic.TopicName,
					"partition", part.Partition,
					"error", err,
				)
				partResp.ErrorCode = protocol.ErrCodeOffsetOutOfRange
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			partResp.HighWatermark = ps.Size()

			cap := part.PartitionMaxBytes
			if remaining < cap {
				cap = remaining
			}

			if cap <= 0 {
				partResp.ErrorCode = protocol.ErrCodeNone
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			r, n, err := ps.Read(uint64(part.FetchOffset), cap)
			if err != nil {
				if errors.Is(err, storage.ErrInvalidOffset) {
					h.logger.Warn("fetch: offset out of range",
						"topic", topic.TopicName,
						"partition", part.Partition,
						"fetch_offset", part.FetchOffset,
					)
					partResp.ErrorCode = protocol.ErrCodeOffsetOutOfRange
				} else {
					h.logger.Error("fetch: storage read failed",
						"topic", topic.TopicName,
						"partition", part.Partition,
						"error", err,
					)
					partResp.ErrorCode = protocol.ErrCodeOffsetOutOfRange
				}
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			partResp.ErrorCode = protocol.ErrCodeNone
			partResp.RecordBatch = r
			partResp.BatchSize = n
			remaining -= n

			h.metrics.MessagesFetchedTotal.WithLabelValues(topic.TopicName).Inc()
			h.logger.Info("fetch: batch served",
				"topic", topic.TopicName,
				"partition", part.Partition,
				"fetch_offset", part.FetchOffset,
				"batch_bytes", n,
			)

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	if err := protocol.WriteFetchResponse(conn, header.CorrelationID, resp); err != nil {
		return fmt.Errorf("handleFetch: write response: %w", err)
	}
	return nil
}

// handleMetadata parses a MetadataRequest and builds a MetadataResponse from
// the live topic-partition map. If the client requests specific topics that do
// not yet exist, they are created on demand. An empty topics list means "all".
func (h *Handler) handleMetadata(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	req, err := dec.ParseMetadataRequest(header)
	if err != nil {
		return fmt.Errorf("handleMetadata: %w", err)
	}

	// If client requested specific topics, ensure they exist.
	for _, t := range req.Topics {
		if _, err := h.store.GetOrCreatePartition(t, 0); err != nil {
			h.logger.Warn("handleMetadata: could not create topic",
				"topic", t, "error", err)
		}
	}

	// Collect all known topic-partitions, grouped by topic.
	tps := h.store.Topics()
	byTopic := make(map[string][]int32, len(tps))
	for _, tp := range tps {
		byTopic[tp.Topic] = append(byTopic[tp.Topic], tp.Partition)
	}

	topicMeta := make([]protocol.TopicMetadata, 0, len(byTopic))
	for topicName, partitions := range byTopic {
		partMeta := make([]protocol.PartitionMetadata, 0, len(partitions))
		for _, pid := range partitions {
			partMeta = append(partMeta, protocol.PartitionMetadata{
				ErrorCode: 0,
				Partition: pid,
				Leader:    1,
				Replicas:  []int32{1},
				Isr:       []int32{1},
			})
		}
		topicMeta = append(topicMeta, protocol.TopicMetadata{
			ErrorCode:  0,
			Name:       topicName,
			Partitions: partMeta,
		})
	}

	resp := &protocol.MetadataResponse{
		Brokers: []protocol.Broker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		Topics: topicMeta,
	}

	enc := protocol.NewEncoder()
	enc.EncodeMetadataResponse(header.CorrelationID, resp)
	if _, err = conn.Write(enc.FullMessage()); err != nil {
		return fmt.Errorf("handleMetadata: failed to write response: %w", err)
	}
	return nil
}

// handleOffsetCommit processes an OffsetCommitRequest (ApiKey 8, v2).
//
// Each committed offset is:
//  1. Persisted to the __consumer_offsets topic in BrokerStore by appending a
//     record whose key encodes [groupIDLen(2)groupID topicLen(2)topic partition(4)]
//     and whose value encodes the committed offset as a big-endian int64.
//  2. Cached in-memory (offsetCache) for fast OffsetFetch responses.
func (h *Handler) handleOffsetCommit(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	req, err := dec.ParseOffsetCommitRequest(header)
	if err != nil {
		return fmt.Errorf("handleOffsetCommit: parse: %w", err)
	}

	resp := &protocol.OffsetCommitResponse{
		Topics: make([]protocol.OffsetCommitTopicResponse, 0, len(req.Topics)),
	}

	for _, topic := range req.Topics {
		topicResp := protocol.OffsetCommitTopicResponse{
			TopicName:  topic.TopicName,
			Partitions: make([]protocol.OffsetCommitPartitionResponse, 0, len(topic.Partitions)),
		}

		for _, part := range topic.Partitions {
			partResp := protocol.OffsetCommitPartitionResponse{
				Partition: part.Partition,
				ErrorCode: 0,
			}

			// Serialize key: groupIDLen(2) + groupID + topicLen(2) + topic + partition(4)
			key := encodeOffsetKey(req.GroupID, topic.TopicName, part.Partition)

			// Serialize value: committed offset as big-endian int64 (8 bytes)
			var valueBuf [8]byte
			binary.BigEndian.PutUint64(valueBuf[:], uint64(part.CommittedOffset))

			// Build the record: key + value with simple length-prefixed framing.
			// Format: keyLen(4) + key + valueLen(4) + value
			record := encodeOffsetRecord(key, valueBuf[:])

			ps, err := h.store.GetOrCreatePartition(protocol.ConsumerOffsetsTopic, 0)
			if err != nil {
				h.logger.Error("offset commit: failed to get partition store",
					"group", req.GroupID, "topic", topic.TopicName,
					"partition", part.Partition, "error", err,
				)
				partResp.ErrorCode = 1
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			if _, err := ps.Append(record); err != nil {
				h.logger.Error("offset commit: storage append failed",
					"group", req.GroupID, "topic", topic.TopicName,
					"partition", part.Partition, "error", err,
				)
				partResp.ErrorCode = 1
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			// Update in-memory cache.
			cacheKey := offsetCacheKey{
				GroupID:   req.GroupID,
				Topic:     topic.TopicName,
				Partition: part.Partition,
			}
			h.offsetMu.Lock()
			h.offsetCache[cacheKey] = part.CommittedOffset
			h.offsetMu.Unlock()

			h.logger.Info("offset commit: stored",
				"group", req.GroupID,
				"topic", topic.TopicName,
				"partition", part.Partition,
				"offset", part.CommittedOffset,
			)
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	enc := protocol.NewEncoder()
	enc.EncodeOffsetCommitResponse(header.CorrelationID, resp)
	if _, err := conn.Write(enc.FullMessage()); err != nil {
		return fmt.Errorf("handleOffsetCommit: write response: %w", err)
	}
	return nil
}

// handleOffsetFetch processes an OffsetFetchRequest (ApiKey 9, v1).
//
// Offsets are served from the in-memory cache populated by handleOffsetCommit.
// If no commit has been recorded for a group/topic/partition, OffsetUnknown (-1)
// is returned — matching the Kafka protocol convention.
func (h *Handler) handleOffsetFetch(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	req, err := dec.ParseOffsetFetchRequest(header)
	if err != nil {
		return fmt.Errorf("handleOffsetFetch: parse: %w", err)
	}

	resp := &protocol.OffsetFetchResponse{
		Topics: make([]protocol.OffsetFetchTopicResponse, 0, len(req.Topics)),
	}

	h.offsetMu.RLock()
	for _, topic := range req.Topics {
		topicResp := protocol.OffsetFetchTopicResponse{
			TopicName:  topic.TopicName,
			Partitions: make([]protocol.OffsetFetchPartitionResponse, 0, len(topic.Partitions)),
		}
		for _, part := range topic.Partitions {
			cacheKey := offsetCacheKey{
				GroupID:   req.GroupID,
				Topic:     topic.TopicName,
				Partition: part.Partition,
			}
			offset, ok := h.offsetCache[cacheKey]
			if !ok {
				offset = protocol.OffsetUnknown
			}
			topicResp.Partitions = append(topicResp.Partitions, protocol.OffsetFetchPartitionResponse{
				Partition:         part.Partition,
				CommittedOffset:   offset,
				CommittedMetadata: "",
				ErrorCode:         0,
			})
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	h.offsetMu.RUnlock()

	enc := protocol.NewEncoder()
	enc.EncodeOffsetFetchResponse(header.CorrelationID, resp)
	if _, err := conn.Write(enc.FullMessage()); err != nil {
		return fmt.Errorf("handleOffsetFetch: write response: %w", err)
	}
	return nil
}

// LoadOffsetsCache reads all records from the __consumer_offsets partition 0
// and populates the in-memory offsetCache. It must be called before accepting
// TCP connections so that committed offsets survive broker restarts.
func (h *Handler) LoadOffsetsCache(ctx context.Context) error {
	ps, err := h.store.GetOrCreatePartition(protocol.ConsumerOffsetsTopic, 0)
	if err != nil {
		// If the partition cannot even be opened it's a real error.
		return fmt.Errorf("LoadOffsetsCache: open partition: %w", err)
	}

	size := ps.Size()
	if size == 0 {
		h.logger.Info("offset hydration: no committed offsets found (fresh broker)")
		return nil
	}

	// Read the entire partition in one shot (maxBytes = int32 max, capped by size).
	maxBytes := int32(size)
	if size > int64(^uint32(0)>>1) {
		maxBytes = int32(^uint32(0) >> 1)
	}

	r, _, err := ps.Read(0, maxBytes)
	if err != nil {
		return fmt.Errorf("LoadOffsetsCache: read partition: %w", err)
	}

	loaded := 0
	var lenBuf [4]byte
	h.offsetMu.Lock()
	defer h.offsetMu.Unlock()

	for {
		// Read the 4-byte record length prefix written by the segment layer.
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("LoadOffsetsCache: read record length: %w", err)
		}
		recLen := binary.BigEndian.Uint32(lenBuf[:])

		rec := make([]byte, recLen)
		if _, err := io.ReadFull(r, rec); err != nil {
			return fmt.Errorf("LoadOffsetsCache: read record body: %w", err)
		}

		cacheKey, offset, err := decodeOffsetRecord(rec)
		if err != nil {
			h.logger.Warn("LoadOffsetsCache: skipping malformed record", "error", err)
			continue
		}

		h.offsetCache[cacheKey] = offset
		loaded++
	}

	h.logger.Info("offset hydration: complete", "loaded_offsets", loaded)
	return nil
}

// decodeOffsetRecord parses the opaque byte slice stored in __consumer_offsets.
// Format: keyLen(4) + key + valueLen(4) + value
// Key format: groupIDLen(2) + groupID + topicLen(2) + topic + partition(4)
// Value: big-endian uint64 committed offset.
func decodeOffsetRecord(record []byte) (offsetCacheKey, int64, error) {
	if len(record) < 4 {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: record too short (%d bytes)", len(record))
	}
	pos := 0

	keyLen := int(binary.BigEndian.Uint32(record[pos:]))
	pos += 4
	if pos+keyLen > len(record) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: key length %d exceeds record", keyLen)
	}
	key := record[pos : pos+keyLen]
	pos += keyLen

	if pos+4 > len(record) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: no room for value length")
	}
	valueLen := int(binary.BigEndian.Uint32(record[pos:]))
	pos += 4
	if pos+valueLen > len(record) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: value length %d exceeds record", valueLen)
	}
	value := record[pos : pos+valueLen]

	// Decode key: groupIDLen(2) + groupID + topicLen(2) + topic + partition(4)
	kpos := 0
	if len(key) < 2 {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: key too short for groupID length")
	}
	groupIDLen := int(binary.BigEndian.Uint16(key[kpos:]))
	kpos += 2
	if kpos+groupIDLen > len(key) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: groupID length %d exceeds key", groupIDLen)
	}
	groupID := string(key[kpos : kpos+groupIDLen])
	kpos += groupIDLen

	if kpos+2 > len(key) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: key too short for topic length")
	}
	topicLen := int(binary.BigEndian.Uint16(key[kpos:]))
	kpos += 2
	if kpos+topicLen > len(key) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: topic length %d exceeds key", topicLen)
	}
	topic := string(key[kpos : kpos+topicLen])
	kpos += topicLen

	if kpos+4 > len(key) {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: key too short for partition")
	}
	partition := int32(binary.BigEndian.Uint32(key[kpos:]))

	if len(value) < 8 {
		return offsetCacheKey{}, 0, fmt.Errorf("decodeOffsetRecord: value too short for offset (%d bytes)", len(value))
	}
	offset := int64(binary.BigEndian.Uint64(value[:8]))

	return offsetCacheKey{GroupID: groupID, Topic: topic, Partition: partition}, offset, nil
}

// encodeOffsetKey serialises a (groupID, topic, partition) tuple into a byte
// slice used as the key of an offset record stored in __consumer_offsets.
// Format: groupIDLen(2) + groupID + topicLen(2) + topic + partition(4)
func encodeOffsetKey(groupID, topic string, partition int32) []byte {
	buf := make([]byte, 2+len(groupID)+2+len(topic)+4)
	pos := 0
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(groupID)))
	pos += 2
	copy(buf[pos:], groupID)
	pos += len(groupID)
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(topic)))
	pos += 2
	copy(buf[pos:], topic)
	pos += len(topic)
	binary.BigEndian.PutUint32(buf[pos:], uint32(partition))
	return buf
}

// encodeOffsetRecord builds the opaque byte slice appended to __consumer_offsets.
// Format: keyLen(4) + key + valueLen(4) + value
func encodeOffsetRecord(key, value []byte) []byte {
	buf := make([]byte, 4+len(key)+4+len(value))
	pos := 0
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(key)))
	pos += 4
	copy(buf[pos:], key)
	pos += len(key)
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(value)))
	pos += 4
	copy(buf[pos:], value)
	return buf
}
