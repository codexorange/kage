package protocol

import (
	"encoding/binary"
	"io"
)

// Decoder is responsible for decoding binary data from an io.Reader.
type Decoder struct {
	reader io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: r}
}

func (d *Decoder) ReadInt16() (int16, error) {
	var value int16
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

func (d *Decoder) ReadInt32() (int32, error) {
	var value int32
	err := binary.Read(d.reader, binary.BigEndian, &value)
	return value, err
}

// In production it is better to pass a buffer pre-assigned to avoid allocations.
func (d *Decoder) ReadBytes(n int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(d.reader, buf)
	return buf, err
}

func (d *Decoder) ReadString() (string, error) {
	length, err := d.ReadInt16()
	if err != nil || length <= 0 {
		return "", err
	}
	bytes, err := d.ReadBytes(int(length))
	return string(bytes), err
}
