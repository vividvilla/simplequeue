package resp

import (
	"bytes"
	"testing"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "PING",
			input: "*1\r\n$4\r\nPING\r\n",
			want:  []string{"PING"},
		},
		{
			name:  "SET key value",
			input: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
			want:  []string{"SET", "key", "value"},
		},
		{
			name:  "LPUSH with multiple values",
			input: "*4\r\n$5\r\nLPUSH\r\n$6\r\norders\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			want:  []string{"LPUSH", "orders", "foo", "bar"},
		},
		{
			name:  "empty array",
			input: "*0\r\n",
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(bytes.NewReader([]byte(tt.input)))
			got, err := r.ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d elements, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("element %d: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReadCommandErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"not an array", "+OK\r\n"},
		{"truncated", "*2\r\n$3\r\nfoo\r\n"},
		{"bad array len", "*abc\r\n"},
		{"bad bulk len", "*1\r\n$abc\r\nfoo\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(bytes.NewReader([]byte(tt.input)))
			_, err := r.ReadCommand()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestWriter(t *testing.T) {
	tests := []struct {
		name string
		fn   func(w *Writer)
		want string
	}{
		{
			name: "simple string",
			fn:   func(w *Writer) { w.WriteSimpleString("OK") },
			want: "+OK\r\n",
		},
		{
			name: "error",
			fn:   func(w *Writer) { w.WriteError("ERR bad command") },
			want: "-ERR bad command\r\n",
		},
		{
			name: "integer",
			fn:   func(w *Writer) { w.WriteInteger(42) },
			want: ":42\r\n",
		},
		{
			name: "bulk string",
			fn:   func(w *Writer) { w.WriteBulkString("hello") },
			want: "$5\r\nhello\r\n",
		},
		{
			name: "null",
			fn:   func(w *Writer) { w.WriteNull() },
			want: "$-1\r\n",
		},
		{
			name: "array header",
			fn:   func(w *Writer) { w.WriteArrayHeader(3) },
			want: "*3\r\n",
		},
		{
			name: "null array",
			fn:   func(w *Writer) { w.WriteNullArray() },
			want: "*-1\r\n",
		},
		{
			name: "zero integer",
			fn:   func(w *Writer) { w.WriteInteger(0) },
			want: ":0\r\n",
		},
		{
			name: "empty bulk string",
			fn:   func(w *Writer) { w.WriteBulkString("") },
			want: "$0\r\n\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			tt.fn(w)
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
