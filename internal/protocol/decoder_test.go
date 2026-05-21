package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestNewDecoder(t *testing.T) {
	r := bytes.NewReader([]byte{})
	d := NewDecoder(r)
	if d == nil {
		t.Fatal("expected non-nil decoder")
	}
}

func TestReadInt16(t *testing.T) {
	tests := []struct {
		name    string
		value   int16
		wantErr bool
	}{
		{"positive", 1234, false},
		{"zero", 0, false},
		{"negative", -1, false},
		{"max", 32767, false},
		{"min", -32768, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 2)
			binary.BigEndian.PutUint16(buf, uint16(tt.value))
			d := NewDecoder(bytes.NewReader(buf))
			got, err := d.ReadInt16()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReadInt16() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.value {
				t.Errorf("ReadInt16() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestReadInt16_EOF(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{0x00})) // only 1 byte, need 2
	_, err := d.ReadInt16()
	if err == nil {
		t.Fatal("expected error on truncated input")
	}
}

func TestReadInt32(t *testing.T) {
	tests := []struct {
		name  string
		value int32
	}{
		{"positive", 100000},
		{"zero", 0},
		{"negative", -42},
		{"max", 2147483647},
		{"min", -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(tt.value))
			d := NewDecoder(bytes.NewReader(buf))
			got, err := d.ReadInt32()
			if err != nil {
				t.Fatalf("ReadInt32() unexpected error: %v", err)
			}
			if got != tt.value {
				t.Errorf("ReadInt32() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestReadInt32_EOF(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{0x00, 0x00})) // only 2 bytes, need 4
	_, err := d.ReadInt32()
	if err == nil {
		t.Fatal("expected error on truncated input")
	}
}

func TestReadBytes(t *testing.T) {
	t.Run("normal read", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04}
		d := NewDecoder(bytes.NewReader(data))
		got, err := d.ReadBytes(4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("ReadBytes() = %v, want %v", got, data)
		}
	})

	t.Run("zero length", func(t *testing.T) {
		d := NewDecoder(bytes.NewReader([]byte{}))
		got, err := d.ReadBytes(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("ReadBytes(0) = %v, want nil", got)
		}
	})

	t.Run("negative length", func(t *testing.T) {
		d := NewDecoder(bytes.NewReader([]byte{}))
		got, err := d.ReadBytes(-1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("ReadBytes(-1) = %v, want nil", got)
		}
	})

	t.Run("truncated read", func(t *testing.T) {
		d := NewDecoder(bytes.NewReader([]byte{0x01, 0x02}))
		_, err := d.ReadBytes(5)
		if err == nil {
			t.Fatal("expected error on truncated input")
		}
	})
}

func TestReadString(t *testing.T) {
	encodeString := func(s string) []byte {
		buf := make([]byte, 2+len(s))
		binary.BigEndian.PutUint16(buf[:2], uint16(len(s)))
		copy(buf[2:], s)
		return buf
	}

	t.Run("normal string", func(t *testing.T) {
		d := NewDecoder(bytes.NewReader(encodeString("hello")))
		got, err := d.ReadString()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "hello" {
			t.Errorf("ReadString() = %q, want %q", got, "hello")
		}
	})

	t.Run("empty string (length=0)", func(t *testing.T) {
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, 0)
		d := NewDecoder(bytes.NewReader(buf))
		got, err := d.ReadString()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "" {
			t.Errorf("ReadString() = %q, want empty", got)
		}
	})

	t.Run("eof on length read", func(t *testing.T) {
		d := NewDecoder(bytes.NewReader([]byte{}))
		_, err := d.ReadString()
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	})

	t.Run("truncated body", func(t *testing.T) {
		// length says 5 but only 2 bytes of body available
		buf := []byte{0x00, 0x05, 0x61, 0x62}
		d := NewDecoder(bytes.NewReader(buf))
		_, err := d.ReadString()
		if err == nil {
			t.Fatal("expected error on truncated string body")
		}
	})
}

func TestDecoder_Sequential(t *testing.T) {
	// Build a buffer with: int32=100, int16=18, int16=3, int32=42
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, int32(100))
	binary.Write(&buf, binary.BigEndian, int16(18))
	binary.Write(&buf, binary.BigEndian, int16(3))
	binary.Write(&buf, binary.BigEndian, int32(42))

	d := NewDecoder(&buf)

	if v, err := d.ReadInt32(); err != nil || v != 100 {
		t.Errorf("first ReadInt32: got %v, err %v", v, err)
	}
	if v, err := d.ReadInt16(); err != nil || v != 18 {
		t.Errorf("ReadInt16 (ApiKey): got %v, err %v", v, err)
	}
	if v, err := d.ReadInt16(); err != nil || v != 3 {
		t.Errorf("ReadInt16 (ApiVersion): got %v, err %v", v, err)
	}
	if v, err := d.ReadInt32(); err != nil || v != 42 {
		t.Errorf("second ReadInt32: got %v, err %v", v, err)
	}
}
