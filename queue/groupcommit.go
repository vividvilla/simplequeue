package queue

import (
	"context"
	"sync"
)

// Mutation is a function that mutates queue state. It runs inside a batch flush.
type Mutation func(state *QueueState) error

// result carries the outcome of a batch flush back to waiting callers.
type result struct {
	err error
}

type pending struct {
	mutate Mutation
	done   chan result
}

// GroupCommitter batches multiple mutations into a single CAS write.
// It uses a "borrowed goroutine" pattern: the first submitter to arrive
// while no flush is in-flight becomes the flusher; others block.
type GroupCommitter struct {
	queue *Queue

	mu       sync.Mutex
	batch    []pending
	flushing bool
}

// NewGroupCommitter creates a GroupCommitter wrapping the given Queue.
func NewGroupCommitter(q *Queue) *GroupCommitter {
	return &GroupCommitter{queue: q}
}

// Submit adds a mutation to the current batch. It blocks until the batch
// containing this mutation is flushed. Returns the error from the flush.
func (gc *GroupCommitter) Submit(ctx context.Context, m Mutation) error {
	p := pending{
		mutate: m,
		done:   make(chan result, 1),
	}

	gc.mu.Lock()
	gc.batch = append(gc.batch, p)
	shouldFlush := !gc.flushing
	if shouldFlush {
		gc.flushing = true
	}
	gc.mu.Unlock()

	if shouldFlush {
		gc.flush(ctx)
	}

	select {
	case r := <-p.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (gc *GroupCommitter) flush(ctx context.Context) {
	for {
		gc.mu.Lock()
		batch := gc.batch
		gc.batch = nil
		if len(batch) == 0 {
			gc.flushing = false
			gc.mu.Unlock()
			return
		}
		gc.mu.Unlock()

		err := gc.queue.casRetry(ctx, func(state *QueueState) error {
			for _, p := range batch {
				if err := p.mutate(state); err != nil {
					// Deliver error to this specific caller, continue batch
					p.done <- result{err: err}
				}
			}
			return nil
		})

		for _, p := range batch {
			select {
			case p.done <- result{err: err}:
			default:
				// Already sent an error from the mutation itself
			}
		}
	}
}
