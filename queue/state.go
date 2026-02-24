// Package queue implements the core distributed queue logic on top of an ObjectStore.
package queue

import (
	"time"
)

// JobStatus represents the lifecycle state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusClaimed   JobStatus = "claimed"
	StatusCompleted JobStatus = "completed"
)

// Job represents a single unit of work in the queue.
type Job struct {
	ID            string    `json:"id"`
	Status        JobStatus `json:"status"`
	Payload       []byte    `json:"payload"`
	ClaimedBy     string    `json:"claimed_by,omitempty"`
	ClaimedAt     time.Time `json:"claimed_at,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	Attempts      int       `json:"attempts"`
}

// QueueState is the per-topic state persisted as a single JSON object.
type QueueState struct {
	Jobs []*Job `json:"jobs"`
}

// BrokerState is the shared broker metadata persisted in broker.json.
type BrokerState struct {
	Broker          string    `json:"broker,omitempty"`
	BrokerHeartbeat time.Time `json:"broker_heartbeat,omitempty"`
}
