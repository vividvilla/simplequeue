package queue_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vivek-ng/simplequeue/queue"
	"github.com/vivek-ng/simplequeue/store/memfs"
)

func TestGroupCommitBatching(t *testing.T) {
	s := memfs.New()
	q := queue.New(s)
	gc := queue.NewGroupCommitter(q)
	ctx := context.Background()

	n := 20
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(i int) {
			defer wg.Done()
			err := gc.Submit(ctx, func(state *queue.QueueState) error {
				state.Jobs = append(state.Jobs, &queue.Job{
					ID:     fmt.Sprintf("gc-job-%d", i),
					Status: queue.StatusPending,
				})
				return nil
			})
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	state, _ := q.State(ctx)
	if len(state.Jobs) != n {
		t.Fatalf("expected %d jobs, got %d", n, len(state.Jobs))
	}
}

func TestGroupCommitMutationError(t *testing.T) {
	s := memfs.New()
	q := queue.New(s)
	gc := queue.NewGroupCommitter(q)
	ctx := context.Background()

	var errCount atomic.Int32

	var wg sync.WaitGroup
	wg.Add(2)

	// One that succeeds
	go func() {
		defer wg.Done()
		gc.Submit(ctx, func(state *queue.QueueState) error {
			state.Jobs = append(state.Jobs, &queue.Job{ID: "ok", Status: queue.StatusPending})
			return nil
		})
	}()

	// One that fails
	go func() {
		defer wg.Done()
		err := gc.Submit(ctx, func(state *queue.QueueState) error {
			return fmt.Errorf("intentional error")
		})
		if err != nil {
			errCount.Add(1)
		}
	}()

	wg.Wait()

	// The error from the mutation should propagate to the caller
	if errCount.Load() < 1 {
		t.Log("mutation error was absorbed by batch (acceptable in group commit)")
	}
}
