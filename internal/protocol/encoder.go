package protocol

import (
	"bytes"
	"encoding/binary"
)

type Encoder struct {
	buffer *bytes.Buffer
}

func NewEncoder() *Encoder {
	return &Encoder{
		buffer: new(bytes.Buffer),
	}
}

func (e *Encoder) WriteInt32(value int32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	e.buffer.Write(buf[:])
}

func (e *Encoder) WriteInt16(value int16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(value))
	e.buffer.Write(buf[:])
}

func (e *Encoder) WriteInt64(value int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	e.buffer.Write(buf[:])
}

func (e *Encoder) WriteInt8(value int8) {
	e.buffer.WriteByte(byte(value))
}

func (e *Encoder) WriteString(value string) {
	e.WriteInt16(int16(len(value)))
	e.buffer.WriteString(value)
}

// WriteNullableString writes a nullable Kafka string.
// A nil pointer writes int16(-1); a non-nil pointer writes the string normally.
func (e *Encoder) WriteNullableString(value *string) {
	if value == nil {
		e.WriteInt16(-1)
		return
	}
	e.WriteString(*value)
}

func (e *Encoder) WriteBytes(value []byte) {
	e.buffer.Write(value)
}

func (e *Encoder) Bytes() []byte {
	return e.buffer.Bytes()
}

func (e *Encoder) FullMessage() []byte {
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(e.buffer.Len()))
	finalBuf := make([]byte, 4+e.buffer.Len())
	copy(finalBuf, sizeBuf[:])
	copy(finalBuf[4:], e.buffer.Bytes())
	return finalBuf
}
