package protocol

import (
	"encoding/binary"
	"testing"
)

func TestNewEncoder(t *testing.T) {
	e := NewEncoder()
	if e == nil {
		t.Fatal("expected non-nil encoder")
	}
	if len(e.Bytes()) != 0 {
		t.Errorf("new encoder should have empty buffer, got %d bytes", len(e.Bytes()))
	}
}

func TestWriteInt32(t *testing.T) {
	tests := []struct {
		name  string
		value int32
	}{
		{"positive", 1000},
		{"zero", 0},
		{"negative", -500},
		{"max", 2147483647},
		{"min", -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder()
			e.WriteInt32(tt.value)
			b := e.Bytes()
			if len(b) != 4 {
				t.Fatalf("expected 4 bytes, got %d", len(b))
			}
			got := int32(binary.BigEndian.Uint32(b))
			if got != tt.value {
				t.Errorf("WriteInt32(%v) encoded as %v", tt.value, got)
			}
		})
	}
}

func TestWriteInt16(t *testing.T) {
	tests := []struct {
		name  string
		value int16
	}{
		{"positive", 500},
		{"zero", 0},
		{"negative", -1},
		{"max", 32767},
		{"min", -32768},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder()
			e.WriteInt16(tt.value)
			b := e.Bytes()
			if len(b) != 2 {
				t.Fatalf("expected 2 bytes, got %d", len(b))
			}
			got := int16(binary.BigEndian.Uint16(b))
			if got != tt.value {
				t.Errorf("WriteInt16(%v) encoded as %v", tt.value, got)
			}
		})
	}
}

func TestWriteBytes(t *testing.T) {
	t.Run("normal bytes", func(t *testing.T) {
		e := NewEncoder()
		data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
		e.WriteBytes(data)
		b := e.Bytes()
		if len(b) != len(data) {
			t.Fatalf("expected %d bytes, got %d", len(data), len(b))
		}
		for i, v := range data {
			if b[i] != v {
				t.Errorf("byte[%d] = %x, want %x", i, b[i], v)
			}
		}
	})

	t.Run("empty bytes", func(t *testing.T) {
		e := NewEncoder()
		e.WriteBytes([]byte{})
		if len(e.Bytes()) != 0 {
			t.Errorf("expected 0 bytes after writing empty slice")
		}
	})

	t.Run("nil bytes", func(t *testing.T) {
		e := NewEncoder()
		e.WriteBytes(nil)
		if len(e.Bytes()) != 0 {
			t.Errorf("expected 0 bytes after writing nil")
		}
	})
}

func TestBytes(t *testing.T) {
	e := NewEncoder()
	e.WriteInt32(1)
	e.WriteInt16(2)
	b := e.Bytes()
	if len(b) != 6 {
		t.Errorf("expected 6 bytes (int32 + int16), got %d", len(b))
	}
}

func TestFullMessage(t *testing.T) {
	t.Run("wraps payload with length prefix", func(t *testing.T) {
		e := NewEncoder()
		e.WriteInt32(42)    // 4 bytes
		e.WriteInt16(7)     // 2 bytes — total payload = 6 bytes

		full := e.FullMessage()
		if len(full) != 10 { // 4 (size prefix) + 6 (payload)
			t.Fatalf("expected 10 bytes, got %d", len(full))
		}

		size := int32(binary.BigEndian.Uint32(full[:4]))
		if size != 6 {
			t.Errorf("size prefix = %d, want 6", size)
		}

		payload := full[4:]
		v32 := int32(binary.BigEndian.Uint32(payload[:4]))
		if v32 != 42 {
			t.Errorf("payload int32 = %d, want 42", v32)
		}
		v16 := int16(binary.BigEndian.Uint16(payload[4:]))
		if v16 != 7 {
			t.Errorf("payload int16 = %d, want 7", v16)
		}
	})

	t.Run("empty encoder produces 4-byte zero size prefix", func(t *testing.T) {
		e := NewEncoder()
		full := e.FullMessage()
		if len(full) != 4 {
			t.Fatalf("expected 4 bytes, got %d", len(full))
		}
		size := int32(binary.BigEndian.Uint32(full))
		if size != 0 {
			t.Errorf("size prefix = %d, want 0", size)
		}
	})
}

func TestEncoder_Sequential(t *testing.T) {
	e := NewEncoder()
	e.WriteInt32(100)
	e.WriteInt16(18)
	e.WriteInt16(3)
	e.WriteInt32(99)
	e.WriteBytes([]byte{0xAB, 0xCD})

	b := e.Bytes()
	// 4 + 2 + 2 + 4 + 2 = 14 bytes
	if len(b) != 14 {
		t.Fatalf("expected 14 bytes, got %d", len(b))
	}

	if v := int32(binary.BigEndian.Uint32(b[0:4])); v != 100 {
		t.Errorf("int32 at 0: got %d", v)
	}
	if v := int16(binary.BigEndian.Uint16(b[4:6])); v != 18 {
		t.Errorf("int16 at 4: got %d", v)
	}
	if v := int16(binary.BigEndian.Uint16(b[6:8])); v != 3 {
		t.Errorf("int16 at 6: got %d", v)
	}
	if v := int32(binary.BigEndian.Uint32(b[8:12])); v != 99 {
		t.Errorf("int32 at 8: got %d", v)
	}
	if b[12] != 0xAB || b[13] != 0xCD {
		t.Errorf("bytes at 12: got %x %x", b[12], b[13])
	}
}
