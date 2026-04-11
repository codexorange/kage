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
	store   storage.Store
	metrics *metrics.Metrics
}

func NewHandler(logger *slog.Logger, store storage.Store, m *metrics.Metrics) *Handler {
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

			start := time.Now()
			offset, err := h.store.Append(part.RecordBatch)
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
	hwm := h.store.Size()

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
				Partition:     part.Partition,
				HighWatermark: hwm,
			}

			cap := part.PartitionMaxBytes
			if remaining < cap {
				cap = remaining
			}

			if cap <= 0 {
				partResp.ErrorCode = protocol.ErrCodeNone
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			r, err := h.store.Read(uint64(part.FetchOffset), cap)
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

			batch, err := io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("handleFetch: read batch: %w", err)
			}

			partResp.ErrorCode = protocol.ErrCodeNone
			partResp.RecordBatch = batch
			remaining -= int32(len(batch))

			h.metrics.MessagesFetchedTotal.WithLabelValues(topic.TopicName).Inc()
			h.logger.Info("fetch: batch served",
				"topic", topic.TopicName,
				"partition", part.Partition,
				"fetch_offset", part.FetchOffset,
				"batch_bytes", len(batch),
			)

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}

	enc := protocol.NewEncoder()
	enc.EncodeFetchResponse(header.CorrelationID, resp)
	if _, err := conn.Write(enc.FullMessage()); err != nil {
		return fmt.Errorf("handleFetch: write response: %w", err)
	}
	return nil
}

// handleMetadata parses a MetadataRequest and responds with a hardcoded
// single-topic, single-partition MetadataResponse.
func (h *Handler) handleMetadata(conn net.Conn, dec *protocol.Decoder, header *protocol.RequestHeader) error {
	_, err := dec.ParseMetadataRequest(header)
	if err != nil {
		return fmt.Errorf("handleMetadata: %w", err)
	}

	resp := &protocol.MetadataResponse{
		Brokers: []protocol.Broker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		Topics: []protocol.TopicMetadata{
			{
				ErrorCode: 0,
				Name:      "kage-events",
				Partitions: []protocol.PartitionMetadata{
					{
						ErrorCode: 0,
						Partition: 0,
						Leader:    1,
						Replicas:  []int32{1},
						Isr:       []int32{1},
					},
				},
			},
		},
	}

	enc := protocol.NewEncoder()
	enc.EncodeMetadataResponse(header.CorrelationID, resp)
	if _, err = conn.Write(enc.FullMessage()); err != nil {
		return fmt.Errorf("handleMetadata: failed to write response: %w", err)
	}
	return nil
}
