package resp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/vivek-ng/simplequeue/queue"
)

// fakeQueue is an in-memory implementation of Queue for testing.
type fakeQueue struct {
	mu   sync.Mutex
	jobs map[string][]*queue.Job // topic -> jobs
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{jobs: make(map[string][]*queue.Job)}
}

func (f *fakeQueue) Enqueue(_ context.Context, topic, id string, payload json.RawMessage) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == "" {
		id = fmt.Sprintf("fake-%d", len(f.jobs[topic]))
	}
	f.jobs[topic] = append(f.jobs[topic], &queue.Job{
		ID:      id,
		Status:  queue.StatusPending,
		Payload: payload,
	})
	return id, nil
}

func (f *fakeQueue) Claim(_ context.Context, topic, workerID string) (*queue.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, j := range f.jobs[topic] {
		if j.Status == queue.StatusPending {
			j.Status = queue.StatusClaimed
			j.ClaimedBy = workerID
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}

func (f *fakeQueue) Ack(_ context.Context, topic, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, j := range f.jobs[topic] {
		if j.ID == jobID && j.Status == queue.StatusClaimed {
			j.Status = queue.StatusCompleted
			return nil
		}
	}
	return fmt.Errorf("job not found or not claimed")
}

func (f *fakeQueue) Status(_ context.Context, topic string) (TopicStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var s TopicStatus
	for _, j := range f.jobs[topic] {
		switch j.Status {
		case queue.StatusPending:
			s.Pending++
		case queue.StatusClaimed:
			s.Claimed++
		case queue.StatusCompleted:
			s.Completed++
		}
		s.Total++
	}
	return s, nil
}

func writeCmd(w *Writer, args ...string) {
	w.WriteArrayHeader(len(args))
	for _, a := range args {
		w.WriteBulkString(a)
	}
	w.Flush()
}

func TestPing(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	go srv.serveConn(ctx, s)

	w := NewWriter(c)
	buf := make([]byte, 64)

	writeCmd(w, "PING")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if got != "+PONG\r\n" {
		t.Fatalf("got %q, want +PONG\\r\\n", got)
	}
}

func TestLPushAndRPop(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	done := make(chan struct{})
	go func() {
		srv.serveConn(ctx, s)
		close(done)
	}()

	w := NewWriter(c)
	r := NewReader(c)
	_ = r

	// LPUSH orders '{"item":"book"}'
	writeCmd(w, "LPUSH", "orders", `{"item":"book"}`)
	buf := make([]byte, 64)
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if got != ":1\r\n" {
		t.Fatalf("LPUSH: got %q, want :1\\r\\n", got)
	}

	// LLEN orders
	writeCmd(w, "LLEN", "orders")
	n, _ = c.Read(buf)
	got = string(buf[:n])
	if got != ":1\r\n" {
		t.Fatalf("LLEN: got %q, want :1\\r\\n", got)
	}

	// RPOP orders
	writeCmd(w, "RPOP", "orders")
	n, _ = c.Read(buf)
	got = string(buf[:n])
	want := "$15\r\n{\"item\":\"book\"}\r\n"
	if got != want {
		t.Fatalf("RPOP: got %q, want %q", got, want)
	}

	// RPOP again -> nil
	writeCmd(w, "RPOP", "orders")
	n, _ = c.Read(buf)
	got = string(buf[:n])
	if got != "$-1\r\n" {
		t.Fatalf("RPOP nil: got %q, want $-1\\r\\n", got)
	}
}

func TestXAddAndXReadGroupAndXAck(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	go srv.serveConn(ctx, s)

	w := NewWriter(c)
	buf := make([]byte, 1024)

	// XADD events * action click user alice
	writeCmd(w, "XADD", "events", "*", "action", "click", "user", "alice")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	// Should be a bulk string with a UUID: $36\r\n<uuid>\r\n
	if len(got) < 5 || got[0] != '$' {
		t.Fatalf("XADD: expected bulk string, got %q", got)
	}

	// XLEN events
	writeCmd(w, "XLEN", "events")
	n, _ = c.Read(buf)
	got = string(buf[:n])
	if got != ":1\r\n" {
		t.Fatalf("XLEN: got %q, want :1\\r\\n", got)
	}

	// XREADGROUP GROUP default worker1 COUNT 1 STREAMS events >
	writeCmd(w, "XREADGROUP", "GROUP", "default", "worker1", "COUNT", "1", "STREAMS", "events", ">")
	n, _ = c.Read(buf)
	got = string(buf[:n])
	// Should start with *1 (one stream)
	if len(got) < 3 || got[:3] != "*1\r" {
		t.Fatalf("XREADGROUP: expected array, got %q", got)
	}

	// Extract the job ID from the XREADGROUP response for XACK
	// The response contains the UUID somewhere in it
	// Let's get it from the fake queue directly
	q.mu.Lock()
	jobID := q.jobs["events"][0].ID
	q.mu.Unlock()

	// XACK events default <jobID>
	writeCmd(w, "XACK", "events", "default", jobID)
	n, _ = c.Read(buf)
	got = string(buf[:n])
	if got != ":1\r\n" {
		t.Fatalf("XACK: got %q, want :1\\r\\n", got)
	}
}

func TestXInfoStream(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	go srv.serveConn(ctx, s)

	w := NewWriter(c)
	buf := make([]byte, 1024)

	// Enqueue a job first
	writeCmd(w, "LPUSH", "mystream", "data")
	c.Read(buf)

	// XINFO STREAM mystream
	writeCmd(w, "XINFO", "STREAM", "mystream")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	// Should start with *8 (4 key-value pairs)
	if len(got) < 3 || got[:3] != "*8\r" {
		t.Fatalf("XINFO STREAM: expected *8 array, got %q", got)
	}
}

func TestUnknownCommand(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	go srv.serveConn(ctx, s)

	w := NewWriter(c)
	buf := make([]byte, 256)

	writeCmd(w, "FLUSHALL")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if got != "-ERR unknown command 'FLUSHALL'\r\n" {
		t.Fatalf("unknown cmd: got %q", got)
	}
}

func TestQuit(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	done := make(chan struct{})
	go func() {
		srv.serveConn(ctx, s)
		close(done)
	}()

	w := NewWriter(c)
	buf := make([]byte, 64)

	writeCmd(w, "QUIT")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if got != "+OK\r\n" {
		t.Fatalf("QUIT: got %q, want +OK\\r\\n", got)
	}

	// Server should close connection
	<-done
}

func TestCommandReturnsEmptyArray(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	go srv.serveConn(ctx, s)

	w := NewWriter(c)
	buf := make([]byte, 64)

	writeCmd(w, "COMMAND")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if got != "*0\r\n" {
		t.Fatalf("COMMAND: got %q, want *0\\r\\n", got)
	}
}

func TestMultiValueLPush(t *testing.T) {
	q := newFakeQueue()
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.Default()
	srv := NewServer(q, log)

	go srv.serveConn(ctx, s)

	w := NewWriter(c)
	buf := make([]byte, 64)

	writeCmd(w, "LPUSH", "q", "a", "b", "c")
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if got != ":3\r\n" {
		t.Fatalf("LPUSH multi: got %q, want :3\\r\\n", got)
	}
}
