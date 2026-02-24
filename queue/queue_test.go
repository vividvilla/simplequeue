package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vivek-ng/simplequeue/internal/clock"
	"github.com/vivek-ng/simplequeue/queue"
	"github.com/vivek-ng/simplequeue/store/memfs"
)

func newTestQueue(t *testing.T) (*queue.Queue, *clock.FakeClock) {
	t.Helper()
	fc := clock.Fake(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	q := queue.New(memfs.New(), queue.WithClock(fc))
	return q, fc
}

func TestEnqueueAndClaim(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "job-1", []byte(`{"task":"hello"}`)); err != nil {
		t.Fatal(err)
	}

	job, err := q.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "job-1" {
		t.Fatalf("expected job-1, got %s", job.ID)
	}
	if job.Status != queue.StatusClaimed {
		t.Fatalf("expected claimed, got %s", job.Status)
	}
	if job.ClaimedBy != "worker-1" {
		t.Fatalf("expected worker-1, got %s", job.ClaimedBy)
	}
	if job.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", job.Attempts)
	}
}

func TestClaimFIFO(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	q.Enqueue(ctx, "a", []byte("1"))
	q.Enqueue(ctx, "b", []byte("2"))
	q.Enqueue(ctx, "c", []byte("3"))

	j1, _ := q.Claim(ctx, "w")
	j2, _ := q.Claim(ctx, "w")
	j3, _ := q.Claim(ctx, "w")

	if j1.ID != "a" || j2.ID != "b" || j3.ID != "c" {
		t.Fatalf("expected FIFO order a,b,c, got %s,%s,%s", j1.ID, j2.ID, j3.ID)
	}
}

func TestClaimEmpty(t *testing.T) {
	q, _ := newTestQueue(t)
	_, err := q.Claim(context.Background(), "w")
	if err != queue.ErrNoJobs {
		t.Fatalf("expected ErrNoJobs, got %v", err)
	}
}

func TestAck(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	q.Enqueue(ctx, "j1", []byte("x"))
	q.Claim(ctx, "w")
	if err := q.Ack(ctx, "j1"); err != nil {
		t.Fatal(err)
	}

	state, _ := q.State(ctx)
	if state.Jobs[0].Status != queue.StatusCompleted {
		t.Fatalf("expected completed, got %s", state.Jobs[0].Status)
	}
}

func TestNack(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()

	q.Enqueue(ctx, "j1", []byte("x"))
	q.Claim(ctx, "w1")
	if err := q.Nack(ctx, "j1"); err != nil {
		t.Fatal(err)
	}

	job, err := q.Claim(ctx, "w2")
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != "j1" {
		t.Fatalf("expected j1 to be reclaimable, got %s", job.ID)
	}
	if job.Attempts != 2 {
		t.Fatalf("expected 2 attempts after nack+reclaim, got %d", job.Attempts)
	}
}

func TestHeartbeat(t *testing.T) {
	q, fc := newTestQueue(t)
	ctx := context.Background()

	q.Enqueue(ctx, "j1", []byte("x"))
	q.Claim(ctx, "w")

	fc.Advance(5 * time.Second)
	if err := q.Heartbeat(ctx, "j1"); err != nil {
		t.Fatal(err)
	}

	state, _ := q.State(ctx)
	if state.Jobs[0].LastHeartbeat.Equal(state.Jobs[0].ClaimedAt) {
		t.Fatal("heartbeat should have advanced beyond claimed_at")
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	s := memfs.New()
	q := queue.New(s)
	ctx := context.Background()
	n := 50

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("job-%d", i)
			if err := q.Enqueue(ctx, id, []byte("data")); err != nil {
				t.Errorf("enqueue %s: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	state, _ := q.State(ctx)
	if len(state.Jobs) != n {
		t.Fatalf("expected %d jobs, got %d", n, len(state.Jobs))
	}
}
