package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/codexorange/kage/internal/metrics"
	"github.com/codexorange/kage/internal/protocol"
	"github.com/codexorange/kage/internal/storage"
)

// Handler routes incoming Kafka requests to the appropriate response builder.
type Handler struct {
	logger  *slog.Logger
	store   *storage.BrokerStore
	metrics *metrics.Metrics
}

func NewHandler(logger *slog.Logger, store *storage.BrokerStore, m *metrics.Metrics) *Handler {
	return &Handler{logger: logger, store: store, metrics: m}
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
				h.metrics.MessagesProducedTotal.WithLabelValues(topic.TopicName).Inc()
				h.logger.Info("produce: batch stored",
					"topic", topic.TopicName,
					"partition", part.Partition,
					"offset", offset,
					"batch_bytes", len(part.RecordBatch),
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
