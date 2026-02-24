package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vivek-ng/simplequeue/broker"
	"github.com/vivek-ng/simplequeue/client"
	"github.com/vivek-ng/simplequeue/internal/clock"
	"github.com/vivek-ng/simplequeue/store/memfs"
)

func setupBroker(t *testing.T) (*broker.Broker, *http.ServeMux, *clock.FakeClock) {
	t.Helper()
	fc := clock.Fake(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	s := memfs.New()

	cfg := broker.DefaultConfig()
	cfg.BrokerID = "test-broker"

	b := broker.New(s, cfg, broker.WithBrokerClock(fc))
	mux := b.TestMux()
	return b, mux, fc
}

func TestBrokerEnqueueAndClaim(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := client.New(srv.URL)
	ctx := context.Background()

	// Enqueue
	if _, err := c.Enqueue(ctx, "job-1", json.RawMessage(`{"hello":"world"}`)); err != nil {
		t.Fatal(err)
	}

	// Status
	status, err := c.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 1 {
		t.Fatalf("expected 1 pending, got %d", status.Pending)
	}

	// Claim
	job, err := c.Claim(ctx, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected a job, got nil")
	}
	if job.ID != "job-1" {
		t.Fatalf("expected job-1, got %s", job.ID)
	}

	// Ack
	if err := c.Ack(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}

	status, _ = c.Status(ctx)
	if status.Completed != 1 {
		t.Fatalf("expected 1 completed, got %d", status.Completed)
	}
}

func TestBrokerNack(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := client.New(srv.URL)
	ctx := context.Background()

	_, _ = c.Enqueue(ctx, "j1", json.RawMessage(`{}`))
	c.Claim(ctx, "w1")
	c.Nack(ctx, "j1")

	// Should be claimable again
	job, err := c.Claim(ctx, "w2")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected reclaimed job")
	}
	if job.Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", job.Attempts)
	}
}

func TestBrokerHeartbeat(t *testing.T) {
	_, mux, fc := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := client.New(srv.URL)
	ctx := context.Background()

	_, _ = c.Enqueue(ctx, "j1", json.RawMessage(`{}`))
	c.Claim(ctx, "w1")

	fc.Advance(5 * time.Second)
	if err := c.Heartbeat(ctx, "j1"); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerClaimEmpty(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := client.New(srv.URL)
	job, err := c.Claim(context.Background(), "w1")
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatal("expected nil job for empty queue")
	}
}

func TestBrokerHealthz(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Fatalf("expected ok, got %q", body["status"])
	}
}

func TestBrokerInvalidRequests(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Enqueue without ID — should auto-generate
	resp, _ := http.Post(srv.URL+"/topics/default/enqueue", "application/json", bytes.NewReader([]byte(`{"payload":"x"}`)))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 for auto-generated id, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Claim without worker_id
	resp, _ = http.Post(srv.URL+"/topics/default/claim", "application/json", bytes.NewReader([]byte(`{}`)))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing worker_id, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Invalid JSON
	resp, _ = http.Post(srv.URL+"/topics/default/enqueue", "application/json", bytes.NewReader([]byte(`not json`)))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid json, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestBrokerMultiTopic(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx := context.Background()

	// Enqueue to topicA
	cA := client.New(srv.URL, client.WithTopic("topicA"))
	if _, err := cA.Enqueue(ctx, "j1", json.RawMessage(`{"topic":"A"}`)); err != nil {
		t.Fatal(err)
	}

	// Claim from topicB — should get nothing
	cB := client.New(srv.URL, client.WithTopic("topicB"))
	job, err := cB.Claim(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Fatal("expected nil job from topicB, got", job.ID)
	}

	// Claim from topicA — should succeed
	job, err = cA.Claim(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected a job from topicA, got nil")
	}
	if job.ID != "j1" {
		t.Fatalf("expected j1, got %s", job.ID)
	}

	// Status should be independent
	statusA, _ := cA.Status(ctx)
	statusB, _ := cB.Status(ctx)
	if statusA.Total != 1 {
		t.Fatalf("topicA: expected total=1, got %d", statusA.Total)
	}
	if statusB.Total != 0 {
		t.Fatalf("topicB: expected total=0, got %d", statusB.Total)
	}
}

func TestBrokerInvalidTopicName(t *testing.T) {
	_, mux, _ := setupBroker(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Path traversal attempt
	resp, _ := http.Post(srv.URL+"/topics/../bad/enqueue", "application/json", bytes.NewReader([]byte(`{"id":"j1","payload":{}}`)))
	// Go's ServeMux cleans the path, so ../bad becomes /bad which won't match the route.
	// But if it does match, the topic validation should reject it.
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 400 or 404 for invalid topic, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Topic with dots
	resp, _ = http.Post(srv.URL+"/topics/bad.topic/enqueue", "application/json", bytes.NewReader([]byte(`{"id":"j1","payload":{}}`)))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for topic with dots, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}
