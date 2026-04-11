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
	binary.Write(e.buffer, binary.BigEndian, value)
}

func (e *Encoder) WriteInt16(value int16) {
	binary.Write(e.buffer, binary.BigEndian, value)
}

func (e *Encoder) WriteInt64(value int64) {
	binary.Write(e.buffer, binary.BigEndian, value)
}

func (e *Encoder) WriteString(value string) {
	e.WriteInt16(int16(len(value)))
	e.buffer.WriteString(value)
}

func (e *Encoder) WriteBytes(value []byte) {
	e.buffer.Write(value)
}

func (e *Encoder) Bytes() []byte {
	return e.buffer.Bytes()
}

func (e *Encoder) FullMessage() []byte {
	finalBuf := new(bytes.Buffer)
	size := int32(e.buffer.Len())
	binary.Write(finalBuf, binary.BigEndian, size)
	finalBuf.Write(e.buffer.Bytes())
	return finalBuf.Bytes()
}
