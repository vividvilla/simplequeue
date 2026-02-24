// Package resp implements RESP2 wire protocol reading and writing.
package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Reader reads RESP2 commands from a buffered stream.
type Reader struct {
	rd *bufio.Reader
}

// NewReader wraps r in a RESP Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{rd: bufio.NewReader(r)}
}

// ReadCommand reads one RESP array-of-bulk-strings command.
// Returns the command as a slice of strings.
func (r *Reader) ReadCommand() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}
	if n < 0 {
		return nil, fmt.Errorf("negative array length: %d", n)
	}

	cmd := make([]string, n)
	for i := range n {
		s, err := r.readBulkString()
		if err != nil {
			return nil, fmt.Errorf("reading element %d: %w", i, err)
		}
		cmd[i] = s
	}
	return cmd, nil
}

func (r *Reader) readBulkString() (string, error) {
	line, err := r.readLine()
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got %q", line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %w", err)
	}
	if n < 0 {
		return "", nil // nil bulk string
	}

	buf := make([]byte, n+2) // data + \r\n
	if _, err := io.ReadFull(r.rd, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// readLine reads a \r\n terminated line and strips the terminator.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.rd.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	return line[:len(line)-1], nil
}

// Writer writes RESP2 responses to a buffered stream.
type Writer struct {
	wr *bufio.Writer
}

// NewWriter wraps w in a RESP Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{wr: bufio.NewWriter(w)}
}

// WriteSimpleString writes +s\r\n.
func (w *Writer) WriteSimpleString(s string) {
	w.wr.WriteByte('+')
	w.wr.WriteString(s)
	w.wr.WriteString("\r\n")
}

// WriteError writes -msg\r\n.
func (w *Writer) WriteError(msg string) {
	w.wr.WriteByte('-')
	w.wr.WriteString(msg)
	w.wr.WriteString("\r\n")
}

// WriteInteger writes :n\r\n.
func (w *Writer) WriteInteger(n int) {
	w.wr.WriteByte(':')
	w.wr.WriteString(strconv.Itoa(n))
	w.wr.WriteString("\r\n")
}

// WriteBulkString writes $len\r\ndata\r\n.
func (w *Writer) WriteBulkString(s string) {
	w.wr.WriteByte('$')
	w.wr.WriteString(strconv.Itoa(len(s)))
	w.wr.WriteString("\r\n")
	w.wr.WriteString(s)
	w.wr.WriteString("\r\n")
}

// WriteNull writes $-1\r\n (nil bulk string).
func (w *Writer) WriteNull() {
	w.wr.WriteString("$-1\r\n")
}

// WriteArrayHeader writes *n\r\n.
func (w *Writer) WriteArrayHeader(n int) {
	w.wr.WriteByte('*')
	w.wr.WriteString(strconv.Itoa(n))
	w.wr.WriteString("\r\n")
}

// WriteNullArray writes *-1\r\n.
func (w *Writer) WriteNullArray() {
	w.wr.WriteString("*-1\r\n")
}

// Flush flushes the underlying buffer.
func (w *Writer) Flush() error {
	return w.wr.Flush()
}
