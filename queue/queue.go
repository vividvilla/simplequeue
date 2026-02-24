package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vivek-ng/simplequeue/internal/clock"
	"github.com/vivek-ng/simplequeue/internal/retry"
	"github.com/vivek-ng/simplequeue/store"
)

const defaultKey = "queue.json"

// Queue provides direct CAS-based operations on queue state in object storage.
type Queue struct {
	store    store.ObjectStore
	key      string
	clock    clock.Clock
	retryCfg retry.Config
}

// Option configures a Queue.
type Option func(*Queue)

func WithKey(key string) Option        { return func(q *Queue) { q.key = key } }
func WithClock(c clock.Clock) Option   { return func(q *Queue) { q.clock = c } }
func WithRetry(cfg retry.Config) Option { return func(q *Queue) { q.retryCfg = cfg } }

// New creates a Queue backed by the given store.
func New(s store.ObjectStore, opts ...Option) *Queue {
	q := &Queue{
		store:    s,
		key:      defaultKey,
		clock:    clock.Real(),
		retryCfg: retry.DefaultConfig(),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

// load reads the current queue state and ETag. Returns empty state if not found.
func (q *Queue) load(ctx context.Context) (*QueueState, string, error) {
	obj, err := q.store.Get(ctx, q.key)
	if errors.Is(err, store.ErrNotFound) {
		return &QueueState{}, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("load queue state: %w", err)
	}
	var state QueueState
	if err := json.Unmarshal(obj.Data, &state); err != nil {
		return nil, "", fmt.Errorf("unmarshal queue state: %w", err)
	}
	return &state, obj.ETag, nil
}

// save persists the state with CAS. Returns the new ETag or ErrPreconditionFail.
func (q *Queue) save(ctx context.Context, state *QueueState, etag string) (string, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal queue state: %w", err)
	}
	if etag == "" {
		return q.store.PutIf(ctx, q.key, data, "")
	}
	return q.store.PutIf(ctx, q.key, data, etag)
}

// casRetry retries the given mutate function using CAS until it succeeds or context is done.
func (q *Queue) casRetry(ctx context.Context, mutate func(state *QueueState) error) error {
	return retry.Do(ctx, q.retryCfg, func(err error) bool {
		return errors.Is(err, store.ErrPreconditionFail)
	}, func(_ int) error {
		state, etag, err := q.load(ctx)
		if err != nil {
			return err
		}
		if err := mutate(state); err != nil {
			return err
		}
		_, err = q.save(ctx, state, etag)
		return err
	})
}

var ErrNoJobs = errors.New("no pending jobs")

// Enqueue adds a new job and returns its ID.
func (q *Queue) Enqueue(ctx context.Context, id string, payload []byte) error {
	return q.casRetry(ctx, func(state *QueueState) error {
		state.Jobs = append(state.Jobs, &Job{
			ID:        id,
			Status:    StatusPending,
			Payload:   payload,
			CreatedAt: q.clock.Now(),
		})
		return nil
	})
}

// Claim finds the oldest pending job, marks it as claimed, and returns it.
func (q *Queue) Claim(ctx context.Context, workerID string) (*Job, error) {
	var claimed *Job
	err := q.casRetry(ctx, func(state *QueueState) error {
		claimed = nil
		for _, j := range state.Jobs {
			if j.Status == StatusPending {
				j.Status = StatusClaimed
				j.ClaimedBy = workerID
				j.ClaimedAt = q.clock.Now()
				j.LastHeartbeat = q.clock.Now()
				j.Attempts++
				claimed = j
				return nil
			}
		}
		return ErrNoJobs
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// Ack marks a claimed job as completed.
func (q *Queue) Ack(ctx context.Context, jobID string) error {
	return q.casRetry(ctx, func(state *QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == jobID && j.Status == StatusClaimed {
				j.Status = StatusCompleted
				return nil
			}
		}
		return fmt.Errorf("job %s not found or not claimed", jobID)
	})
}

// Nack returns a claimed job back to pending.
func (q *Queue) Nack(ctx context.Context, jobID string) error {
	return q.casRetry(ctx, func(state *QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == jobID && j.Status == StatusClaimed {
				j.Status = StatusPending
				j.ClaimedBy = ""
				j.ClaimedAt = q.clock.Now()
				return nil
			}
		}
		return fmt.Errorf("job %s not found or not claimed", jobID)
	})
}

// Heartbeat updates the heartbeat timestamp for a claimed job.
func (q *Queue) Heartbeat(ctx context.Context, jobID string) error {
	return q.casRetry(ctx, func(state *QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == jobID && j.Status == StatusClaimed {
				j.LastHeartbeat = q.clock.Now()
				return nil
			}
		}
		return fmt.Errorf("job %s not found or not claimed", jobID)
	})
}

// State returns a snapshot of the current queue state.
func (q *Queue) State(ctx context.Context) (*QueueState, error) {
	state, _, err := q.load(ctx)
	return state, err
}

// Load exposes load for the broker layer.
func (q *Queue) Load(ctx context.Context) (*QueueState, string, error) {
	return q.load(ctx)
}

// Save exposes save for the broker layer.
func (q *Queue) Save(ctx context.Context, state *QueueState, etag string) (string, error) {
	return q.save(ctx, state, etag)
}

// Key returns the storage key.
func (q *Queue) Key() string { return q.key }

// Store returns the underlying store.
func (q *Queue) Store() store.ObjectStore { return q.store }
