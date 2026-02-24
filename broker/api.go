package broker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"

	"github.com/vivek-ng/simplequeue/queue"
)

// TopicStatus holds counts for a topic's job states.
type TopicStatus struct {
	Pending   int
	Claimed   int
	Completed int
	Total     int
}

// ValidTopic reports whether name is a valid topic identifier.
func ValidTopic(name string) bool {
	return validTopic.MatchString(name)
}

// Enqueue adds a job with the given ID and payload to the named topic.
// If id is empty, a random UUID is generated.
func (b *Broker) Enqueue(ctx context.Context, topic, id string, payload json.RawMessage) (string, error) {
	if !ValidTopic(topic) {
		return "", fmt.Errorf("invalid topic name: %q", topic)
	}
	if id == "" {
		id = newUUID()
	}
	entry := b.getOrCreateTopic(topic)
	err := entry.gc.Submit(ctx, func(state *queue.QueueState) error {
		state.Jobs = append(state.Jobs, &queue.Job{
			ID:        id,
			Status:    queue.StatusPending,
			Payload:   payload,
			CreatedAt: b.clock.Now(),
		})
		return nil
	})
	return id, err
}

// newUUID generates a random UUID v4 string.
func newUUID() string {
	var buf [16]byte
	rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 2
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// Claim claims the next pending job from the named topic.
// Returns nil, nil when there are no pending jobs.
func (b *Broker) Claim(ctx context.Context, topic, workerID string) (*queue.Job, error) {
	if !ValidTopic(topic) {
		return nil, fmt.Errorf("invalid topic name: %q", topic)
	}
	entry := b.getOrCreateTopic(topic)
	var claimed *queue.Job
	err := entry.gc.Submit(ctx, func(state *queue.QueueState) error {
		for _, j := range state.Jobs {
			if j.Status == queue.StatusPending {
				j.Status = queue.StatusClaimed
				j.ClaimedBy = workerID
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
	return claimed, err
}

// Ack marks a claimed job as completed.
func (b *Broker) Ack(ctx context.Context, topic, jobID string) error {
	if !ValidTopic(topic) {
		return fmt.Errorf("invalid topic name: %q", topic)
	}
	entry := b.getOrCreateTopic(topic)
	return entry.gc.Submit(ctx, func(state *queue.QueueState) error {
		for _, j := range state.Jobs {
			if j.ID == jobID && j.Status == queue.StatusClaimed {
				j.Status = queue.StatusCompleted
				return nil
			}
		}
		return nil
	})
}

// Status returns job counts for the named topic.
func (b *Broker) Status(ctx context.Context, topic string) (TopicStatus, error) {
	if !ValidTopic(topic) {
		return TopicStatus{}, fmt.Errorf("invalid topic name: %q", topic)
	}
	entry := b.getOrCreateTopic(topic)
	state, err := entry.queue.State(ctx)
	if err != nil {
		return TopicStatus{}, err
	}
	var s TopicStatus
	for _, j := range state.Jobs {
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
