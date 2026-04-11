package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/codexorange/kage/internal/protocol"
)

// buildMetadataRequestFrame builds a complete on-wire MetadataRequest frame.
func buildMetadataRequestFrame(correlationID int32, clientID string, topics []string) []byte {
	var body bytes.Buffer

	// Header fields after Size: ApiKey, ApiVersion, CorrelationID, ClientID.
	binary.Write(&body, binary.BigEndian, int16(protocol.ApiKeyMetadata))
	binary.Write(&body, binary.BigEndian, int16(0)) // version
	binary.Write(&body, binary.BigEndian, correlationID)
	binary.Write(&body, binary.BigEndian, int16(len(clientID)))
	body.WriteString(clientID)

	// Body: topics array.
	binary.Write(&body, binary.BigEndian, int32(len(topics)))
	for _, t := range topics {
		binary.Write(&body, binary.BigEndian, int16(len(t)))
		body.WriteString(t)
	}

	var frame bytes.Buffer
	binary.Write(&frame, binary.BigEndian, int32(body.Len()))
	frame.Write(body.Bytes())
	return frame.Bytes()
}

// pipeConn wraps a net.Pipe connection for testing.
type pipeConn struct {
	net.Conn
}

func TestHandler_MetadataRequest(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(logger)

	go h.Handle(serverConn)

	// Send a MetadataRequest.
	frame := buildMetadataRequestFrame(99, "test-client", []string{"kage-events"})
	_, err := clientConn.Write(frame)
	if err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Read the response (4-byte size prefix + body).
	clientConn.SetDeadline(time.Now().Add(2 * time.Second))

	var sizePrefix int32
	if err := binary.Read(clientConn, binary.BigEndian, &sizePrefix); err != nil {
		t.Fatalf("failed to read response size: %v", err)
	}
	if sizePrefix <= 0 {
		t.Fatalf("response size = %d, want > 0", sizePrefix)
	}

	body := make([]byte, sizePrefix)
	if _, err := io.ReadFull(clientConn, body); err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	dec := protocol.NewDecoder(bytes.NewReader(body))

	corrID, _ := dec.ReadInt32()
	if corrID != 99 {
		t.Errorf("correlationID = %d, want 99", corrID)
	}

	brokerCount, _ := dec.ReadInt32()
	if brokerCount != 1 {
		t.Errorf("broker count = %d, want 1", brokerCount)
	}

	// Skip broker fields: NodeID(4) + host string + port(4).
	dec.ReadInt32() // NodeID
	dec.ReadString() // host
	dec.ReadInt32() // port

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

	partCount, _ := dec.ReadInt32()
	if partCount != 1 {
		t.Errorf("partition count = %d, want 1", partCount)
	}
}

func TestHandler_UnsupportedApiKey(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(logger)

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Handle(serverConn)
	}()

	// Send a request with an unsupported ApiKey (e.g. 99).
	var frame bytes.Buffer
	body := new(bytes.Buffer)
	binary.Write(body, binary.BigEndian, int16(99)) // ApiKey
	binary.Write(body, binary.BigEndian, int16(0))  // ApiVersion
	binary.Write(body, binary.BigEndian, int32(1))  // CorrelationID
	binary.Write(body, binary.BigEndian, int16(0))  // ClientID (empty)
	binary.Write(&frame, binary.BigEndian, int32(body.Len()))
	frame.Write(body.Bytes())

	clientConn.Write(frame.Bytes())

	// Handler should close the connection after logging the error.
	clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err := clientConn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed by handler, but read succeeded")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after unsupported ApiKey")
	}
}
