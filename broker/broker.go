// Package broker implements a stateless HTTP broker for the distributed queue.
package broker

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/vivek-ng/simplequeue/internal/clock"
	"github.com/vivek-ng/simplequeue/internal/retry"
	"github.com/vivek-ng/simplequeue/queue"
	"github.com/vivek-ng/simplequeue/store"
)

var validTopic = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Config holds broker configuration.
type Config struct {
	Addr            string
	BrokerID        string
	HeartbeatPeriod time.Duration
	StaleJobTimeout time.Duration
	ReaperPeriod    time.Duration
	ShutdownTimeout time.Duration
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Addr:            ":8080",
		HeartbeatPeriod: 10 * time.Second,
		StaleJobTimeout: 30 * time.Second,
		ReaperPeriod:    15 * time.Second,
		ShutdownTimeout: 10 * time.Second,
	}
}

// topicEntry holds the queue and group committer for a single topic.
type topicEntry struct {
	queue *queue.Queue
	gc    *queue.GroupCommitter
}

// Broker is a stateless queue broker that reads/writes state to object storage.
type Broker struct {
	cfg      Config
	store    store.ObjectStore
	clock    clock.Clock
	log      *slog.Logger
	retryCfg *retry.Config

	topics map[string]*topicEntry
	mu     sync.RWMutex

	server *http.Server
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// BrokerOption configures a Broker.
type BrokerOption func(*Broker)

func WithBrokerClock(c clock.Clock) BrokerOption { return func(b *Broker) { b.clock = c } }
func WithLogger(l *slog.Logger) BrokerOption     { return func(b *Broker) { b.log = l } }
func WithRetry(cfg retry.Config) BrokerOption    { return func(b *Broker) { b.retryCfg = &cfg } }

// New creates a new Broker.
func New(s store.ObjectStore, cfg Config, opts ...BrokerOption) *Broker {
	b := &Broker{
		cfg:    cfg,
		store:  s,
		clock:  clock.Real(),
		log:    slog.Default(),
		topics: make(map[string]*topicEntry),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// getOrCreateTopic returns the topicEntry for the given topic name,
// creating it lazily if it doesn't exist. Uses double-checked locking.
func (b *Broker) getOrCreateTopic(topic string) *topicEntry {
	b.mu.RLock()
	entry, ok := b.topics[topic]
	b.mu.RUnlock()
	if ok {
		return entry
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double-check after acquiring write lock.
	if entry, ok = b.topics[topic]; ok {
		return entry
	}

	opts := []queue.Option{
		queue.WithKey(topic + ".json"),
		queue.WithClock(b.clock),
	}
	if b.retryCfg != nil {
		opts = append(opts, queue.WithRetry(*b.retryCfg))
	}

	q := queue.New(b.store, opts...)
	entry = &topicEntry{
		queue: q,
		gc:    queue.NewGroupCommitter(q),
	}
	b.topics[topic] = entry
	return entry
}

// Start begins serving HTTP, heartbeating, and reaping stale jobs.
func (b *Broker) Start(ctx context.Context) error {
	ctx, b.cancel = context.WithCancel(ctx)

	mux := http.NewServeMux()
	b.registerRoutes(mux)

	b.server = &http.Server{
		Addr:    b.cfg.Addr,
		Handler: mux,
	}

	// Register as broker
	if err := b.registerBroker(ctx); err != nil {
		return err
	}

	// Broker heartbeat loop
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.heartbeatLoop(ctx)
	}()

	// Stale job reaper
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.reaperLoop(ctx)
	}()

	// HTTP server
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		if err := b.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			b.log.Error("http server error", "err", err)
		}
	}()

	return nil
}

// TestMux returns an http.ServeMux with broker routes registered, for testing.
func (b *Broker) TestMux() *http.ServeMux {
	mux := http.NewServeMux()
	b.registerRoutes(mux)
	return mux
}

// Stop gracefully shuts down the broker.
func (b *Broker) Stop() error {
	b.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), b.cfg.ShutdownTimeout)
	defer cancel()

	err := b.server.Shutdown(ctx)
	b.wg.Wait()
	return err
}

func (b *Broker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(b.cfg.HeartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.registerBroker(ctx); err != nil {
				b.log.Warn("broker heartbeat failed", "err", err)
			}
		}
	}
}

func (b *Broker) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(b.cfg.ReaperPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.reapAllTopics(ctx)
		}
	}
}

func (b *Broker) reapAllTopics(ctx context.Context) {
	b.mu.RLock()
	snapshot := make(map[string]*topicEntry, len(b.topics))
	for k, v := range b.topics {
		snapshot[k] = v
	}
	b.mu.RUnlock()

	for topic, entry := range snapshot {
		if err := b.reapStaleJobs(ctx, entry); err != nil {
			b.log.Warn("reaper failed", "topic", topic, "err", err)
		}
	}
}

func (b *Broker) reapStaleJobs(ctx context.Context, entry *topicEntry) error {
	state, etag, err := entry.queue.Load(ctx)
	if err != nil {
		return err
	}

	now := b.clock.Now()
	changed := false
	for _, j := range state.Jobs {
		if j.Status == queue.StatusClaimed && now.Sub(j.LastHeartbeat) > b.cfg.StaleJobTimeout {
			j.Status = queue.StatusPending
			j.ClaimedBy = ""
			b.log.Info("reaped stale job", "job_id", j.ID, "last_heartbeat", j.LastHeartbeat)
			changed = true
		}
	}

	if !changed {
		return nil
	}

	_, err = entry.queue.Save(ctx, state, etag)
	return err
}
