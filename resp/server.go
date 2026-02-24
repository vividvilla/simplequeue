package resp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/vivek-ng/simplequeue/queue"
)

// TopicStatus mirrors broker.TopicStatus to avoid a circular import.
type TopicStatus struct {
	Pending   int
	Claimed   int
	Completed int
	Total     int
}

// Queue defines the broker operations the RESP server needs.
type Queue interface {
	Enqueue(ctx context.Context, topic, id string, payload json.RawMessage) (string, error)
	Claim(ctx context.Context, topic, workerID string) (*queue.Job, error)
	Ack(ctx context.Context, topic, jobID string) error
	Status(ctx context.Context, topic string) (TopicStatus, error)
}

// Server is a Redis-compatible TCP server that translates RESP commands
// into queue operations via the Queue interface.
type Server struct {
	q   Queue
	ln  net.Listener
	log *slog.Logger
	wg  sync.WaitGroup
}

// NewServer creates a RESP server backed by q.
func NewServer(q Queue, log *slog.Logger) *Server {
	return &Server{q: q, log: log}
}

// ListenAndServe starts listening on addr and serving connections.
// It blocks until ctx is cancelled or the listener is closed.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.log.Info("resp server listening", "addr", addr)

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
}

// Close stops accepting and waits for in-flight connections.
func (s *Server) Close() {
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	r := NewReader(conn)
	w := NewWriter(conn)

	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			return // client disconnected or protocol error
		}
		if len(cmd) == 0 {
			continue
		}

		verb := strings.ToUpper(cmd[0])
		switch verb {
		case "PING":
			w.WriteSimpleString("PONG")
		case "QUIT":
			w.WriteSimpleString("OK")
			w.Flush()
			return
		case "COMMAND":
			w.WriteArrayHeader(0)
		case "LPUSH", "RPUSH":
			s.handlePush(ctx, w, cmd)
		case "LPOP", "RPOP":
			s.handlePop(ctx, w, cmd)
		case "LLEN":
			s.handleLLen(ctx, w, cmd)
		case "XADD":
			s.handleXAdd(ctx, w, cmd)
		case "XLEN":
			s.handleXLen(ctx, w, cmd)
		case "XREADGROUP":
			s.handleXReadGroup(ctx, w, cmd)
		case "XACK":
			s.handleXAck(ctx, w, cmd)
		case "XINFO":
			s.handleXInfo(ctx, w, cmd)
		default:
			w.WriteError("ERR unknown command '" + verb + "'")
		}
		w.Flush()
	}
}

// handlePush: LPUSH/RPUSH key value [value...]
func (s *Server) handlePush(ctx context.Context, w *Writer, cmd []string) {
	if len(cmd) < 3 {
		w.WriteError("ERR wrong number of arguments for '" + cmd[0] + "' command")
		return
	}
	topic := cmd[1]
	count := 0
	for _, val := range cmd[2:] {
		if _, err := s.q.Enqueue(ctx, topic, "", json.RawMessage(val)); err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		count++
	}
	w.WriteInteger(count)
}

// handlePop: LPOP/RPOP key
func (s *Server) handlePop(ctx context.Context, w *Writer, cmd []string) {
	if len(cmd) < 2 {
		w.WriteError("ERR wrong number of arguments for '" + cmd[0] + "' command")
		return
	}
	topic := cmd[1]
	job, err := s.q.Claim(ctx, topic, "redis-cli")
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	if job == nil {
		w.WriteNull()
		return
	}
	w.WriteBulkString(string(job.Payload))
}

// handleLLen: LLEN key
func (s *Server) handleLLen(ctx context.Context, w *Writer, cmd []string) {
	if len(cmd) < 2 {
		w.WriteError("ERR wrong number of arguments for 'LLEN' command")
		return
	}
	st, err := s.q.Status(ctx, cmd[1])
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteInteger(st.Pending)
}

// handleXAdd: XADD key * field value [field value...]
func (s *Server) handleXAdd(ctx context.Context, w *Writer, cmd []string) {
	// XADD key * field value [field value...]
	if len(cmd) < 5 || cmd[2] != "*" || (len(cmd)-3)%2 != 0 {
		w.WriteError("ERR wrong number of arguments for 'XADD' command")
		return
	}
	topic := cmd[1]
	fields := make(map[string]string)
	for i := 3; i < len(cmd); i += 2 {
		fields[cmd[i]] = cmd[i+1]
	}
	payload, _ := json.Marshal(fields)
	id, err := s.q.Enqueue(ctx, topic, "", json.RawMessage(payload))
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteBulkString(id)
}

// handleXLen: XLEN key
func (s *Server) handleXLen(ctx context.Context, w *Writer, cmd []string) {
	if len(cmd) < 2 {
		w.WriteError("ERR wrong number of arguments for 'XLEN' command")
		return
	}
	st, err := s.q.Status(ctx, cmd[1])
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteInteger(st.Total)
}

// handleXReadGroup: XREADGROUP GROUP g consumer COUNT n STREAMS key >
func (s *Server) handleXReadGroup(ctx context.Context, w *Writer, cmd []string) {
	// Minimal parse: XREADGROUP GROUP g consumer COUNT n STREAMS key >
	group, consumer, countStr, streamKey, err := parseXReadGroup(cmd)
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	_ = group // we don't use group semantics
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		w.WriteError("ERR invalid COUNT")
		return
	}

	// Claim up to count jobs
	var jobs []*queue.Job
	for range count {
		job, err := s.q.Claim(ctx, streamKey, consumer)
		if err != nil {
			break
		}
		if job == nil {
			break
		}
		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		w.WriteNullArray()
		return
	}

	// Response: array of [streamKey, array of entries]
	// Each entry: [id, [field, value, field, value...]]
	w.WriteArrayHeader(1)          // one stream
	w.WriteArrayHeader(2)          // [key, entries]
	w.WriteBulkString(streamKey)   // stream key
	w.WriteArrayHeader(len(jobs))  // entries array
	for _, job := range jobs {
		// Parse payload back to fields
		var fields map[string]string
		json.Unmarshal(job.Payload, &fields)

		w.WriteArrayHeader(2) // [id, fields]
		w.WriteBulkString(job.ID)
		w.WriteArrayHeader(len(fields) * 2)
		for k, v := range fields {
			w.WriteBulkString(k)
			w.WriteBulkString(v)
		}
	}
}

// handleXAck: XACK key group id [id...]
func (s *Server) handleXAck(ctx context.Context, w *Writer, cmd []string) {
	if len(cmd) < 4 {
		w.WriteError("ERR wrong number of arguments for 'XACK' command")
		return
	}
	topic := cmd[1]
	// cmd[2] is group, ignored
	acked := 0
	for _, id := range cmd[3:] {
		if err := s.q.Ack(ctx, topic, id); err == nil {
			acked++
		}
	}
	w.WriteInteger(acked)
}

// handleXInfo: XINFO STREAM key
func (s *Server) handleXInfo(ctx context.Context, w *Writer, cmd []string) {
	if len(cmd) < 3 || strings.ToUpper(cmd[1]) != "STREAM" {
		w.WriteError("ERR syntax error")
		return
	}
	st, err := s.q.Status(ctx, cmd[2])
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	// Flat key-value array like Redis
	w.WriteArrayHeader(8)
	w.WriteBulkString("length")
	w.WriteInteger(st.Total)
	w.WriteBulkString("pending")
	w.WriteInteger(st.Pending)
	w.WriteBulkString("claimed")
	w.WriteInteger(st.Claimed)
	w.WriteBulkString("completed")
	w.WriteInteger(st.Completed)
}

// parseXReadGroup extracts group, consumer, count, and stream key from the command.
// Expected: XREADGROUP GROUP g consumer COUNT n STREAMS key >
func parseXReadGroup(cmd []string) (group, consumer, count, key string, err error) {
	if len(cmd) < 9 {
		return "", "", "", "", fmt.Errorf("wrong number of arguments for 'XREADGROUP' command")
	}
	if strings.ToUpper(cmd[1]) != "GROUP" {
		return "", "", "", "", fmt.Errorf("syntax error: expected GROUP")
	}
	group = cmd[2]
	consumer = cmd[3]
	if strings.ToUpper(cmd[4]) != "COUNT" {
		return "", "", "", "", fmt.Errorf("syntax error: expected COUNT")
	}
	count = cmd[5]
	if strings.ToUpper(cmd[6]) != "STREAMS" {
		return "", "", "", "", fmt.Errorf("syntax error: expected STREAMS")
	}
	key = cmd[7]
	return group, consumer, count, key, nil
}

