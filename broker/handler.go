package broker

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/vivek-ng/simplequeue/queue"
)

func (b *Broker) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /topics/{topic}/enqueue", b.handleEnqueue)
	mux.HandleFunc("POST /topics/{topic}/claim", b.handleClaim)
	mux.HandleFunc("POST /topics/{topic}/ack", b.handleAck)
	mux.HandleFunc("POST /topics/{topic}/nack", b.handleNack)
	mux.HandleFunc("POST /topics/{topic}/heartbeat", b.handleHeartbeat)
	mux.HandleFunc("GET /topics/{topic}/status", b.handleStatus)
	mux.HandleFunc("GET /healthz", b.handleHealthz)
}

// topicFromRequest extracts and validates the topic path parameter.
// Returns the topicEntry and true on success, or writes a 400 error and returns false.
func (b *Broker) topicFromRequest(w http.ResponseWriter, r *http.Request) (*topicEntry, bool) {
	topic := r.PathValue("topic")
	if !validTopic.MatchString(topic) {
		http.Error(w, `{"error":"invalid topic name"}`, http.StatusBadRequest)
		return nil, false
	}
	return b.getOrCreateTopic(topic), true
}

type enqueueRequest struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type enqueueResponse struct {
	ID string `json:"id"`
}

func (b *Broker) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if !validTopic.MatchString(topic) {
		http.Error(w, `{"error":"invalid topic name"}`, http.StatusBadRequest)
		return
	}

	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	id, err := b.Enqueue(r.Context(), topic, req.ID, req.Payload)
	if err != nil {
		b.log.Error("enqueue failed", "err", err)
		http.Error(w, `{"error":"enqueue failed"}`, http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, enqueueResponse{ID: id})
}

type claimRequest struct {
	WorkerID string `json:"worker_id"`
}

type claimResponse struct {
	Job *queue.Job `json:"job"`
}

func (b *Broker) handleClaim(w http.ResponseWriter, r *http.Request) {
	entry, ok := b.topicFromRequest(w, r)
	if !ok {
		return
	}

	var req claimRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.WorkerID == "" {
		http.Error(w, `{"error":"worker_id is required"}`, http.StatusBadRequest)
		return
	}

	var claimed *queue.Job
	err := entry.gc.Submit(r.Context(), func(state *queue.QueueState) error {
		for _, j := range state.Jobs {
			if j.Status == queue.StatusPending {
				j.Status = queue.StatusClaimed
				j.ClaimedBy = req.WorkerID
				j.ClaimedAt = b.clock.Now()
				j.LastHeartbeat = b.clock.Now()
				j.Attempts++
				claimed = &queue.Job{}
				*claimed = *j
				return nil
			}
		}
		return nil
	})
	if err != nil {
		b.log.Error("claim failed", "err", err)
		http.Error(w, `{"error":"claim failed"}`, http.StatusInternalServerError)
		return
	}

	if claimed == nil {
		http.Error(w, `{"error":"no pending jobs"}`, http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, claimResponse{Job: claimed})
}

type jobIDRequest struct {
	JobID string `json:"job_id"`
}

func (b *Broker) handleAck(w http.ResponseWriter, r *http.Request) {
	entry, ok := b.topicFromRequest(w, r)
	if !ok {
		return
	}

	var req jobIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	err := entry.gc.Submit(r.Context(), func(state *queue.QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == req.JobID && j.Status == queue.StatusClaimed {
				j.Status = queue.StatusCompleted
				return nil
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, `{"error":"ack failed"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (b *Broker) handleNack(w http.ResponseWriter, r *http.Request) {
	entry, ok := b.topicFromRequest(w, r)
	if !ok {
		return
	}

	var req jobIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	err := entry.gc.Submit(r.Context(), func(state *queue.QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == req.JobID && j.Status == queue.StatusClaimed {
				j.Status = queue.StatusPending
				j.ClaimedBy = ""
				return nil
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, `{"error":"nack failed"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type heartbeatRequest struct {
	JobID string `json:"job_id"`
}

func (b *Broker) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	entry, ok := b.topicFromRequest(w, r)
	if !ok {
		return
	}

	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	err := entry.gc.Submit(r.Context(), func(state *queue.QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == req.JobID && j.Status == queue.StatusClaimed {
				j.LastHeartbeat = b.clock.Now()
				return nil
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, `{"error":"heartbeat failed"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type statusResponse struct {
	Pending   int `json:"pending"`
	Claimed   int `json:"claimed"`
	Completed int `json:"completed"`
	Total     int `json:"total"`
}

func (b *Broker) handleStatus(w http.ResponseWriter, r *http.Request) {
	entry, ok := b.topicFromRequest(w, r)
	if !ok {
		return
	}

	state, err := entry.queue.State(r.Context())
	if err != nil {
		http.Error(w, `{"error":"failed to read state"}`, http.StatusInternalServerError)
		return
	}

	var resp statusResponse
	for _, j := range state.Jobs {
		switch j.Status {
		case queue.StatusPending:
			resp.Pending++
		case queue.StatusClaimed:
			resp.Claimed++
		case queue.StatusCompleted:
			resp.Completed++
		}
		resp.Total++
	}
	writeJSON(w, http.StatusOK, resp)
}

func (b *Broker) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "broker_id": b.cfg.BrokerID, "time": time.Now().UTC().Format(time.RFC3339)})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
