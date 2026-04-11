package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/codexorange/kage/internal/protocol"
)

// Handler routes incoming Kafka requests to the appropriate response builder.
type Handler struct {
	logger *slog.Logger
}

func NewHandler(logger *slog.Logger) *Handler {
	return &Handler{logger: logger}
}

// Handle reads a single request from conn, routes it by ApiKey, and writes
// the encoded response back. It loops until the connection is closed.
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
	case protocol.ApiKeyMetadata:
		return h.handleMetadata(conn, dec, header)
	default:
		return fmt.Errorf("unsupported api_key: %d", header.ApiKey)
	}
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

	_, err = conn.Write(enc.FullMessage())
	if err != nil {
		return fmt.Errorf("handleMetadata: failed to write response: %w", err)
	}
	return nil
}
