// Command simplequeued runs the simplequeue broker daemon.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vivek-ng/simplequeue/broker"
	"github.com/vivek-ng/simplequeue/queue"
	"github.com/vivek-ng/simplequeue/resp"
	qstore "github.com/vivek-ng/simplequeue/store"
	"github.com/vivek-ng/simplequeue/store/localfs"
	"github.com/vivek-ng/simplequeue/store/memfs"
	s3store "github.com/vivek-ng/simplequeue/store/s3"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis-addr", "", "Redis-compatible listener address (e.g. :6379)")
	brokerID := flag.String("broker-id", "", "unique broker identifier (default: addr)")
	backend := flag.String("backend", "memfs", "storage backend: memfs, localfs, or s3")
	dir := flag.String("dir", "", "storage directory (required for localfs backend)")
	bucket := flag.String("bucket", "", "S3 bucket name (required for s3 backend)")
	flag.Parse()

	if *brokerID == "" {
		*brokerID = *addr
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var store qstore.ObjectStore
	switch *backend {
	case "memfs":
		store = memfs.New()
		log.Info("using in-memory backend")
	case "localfs":
		if *dir == "" {
			fmt.Fprintln(os.Stderr, "error: -dir is required for localfs backend")
			os.Exit(1)
		}
		var err error
		store, err = localfs.New(*dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating localfs store: %v\n", err)
			os.Exit(1)
		}
		log.Info("using local filesystem backend", "dir", *dir)
	case "s3":
		if *bucket == "" {
			fmt.Fprintln(os.Stderr, "error: -bucket is required for s3 backend")
			os.Exit(1)
		}
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading AWS config: %v\n", err)
			os.Exit(1)
		}
		store = s3store.New(s3.NewFromConfig(cfg), *bucket)
		log.Info("using S3 backend", "bucket", *bucket)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown backend %q\n", *backend)
		os.Exit(1)
	}

	cfg := broker.DefaultConfig()
	cfg.Addr = *addr
	cfg.BrokerID = *brokerID

	b := broker.New(store, cfg, broker.WithLogger(log))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := b.Start(ctx); err != nil {
		log.Error("failed to start broker", "err", err)
		os.Exit(1)
	}
	log.Info("broker started", "addr", *addr, "broker_id", *brokerID)

	var rs *resp.Server
	if *redisAddr != "" {
		rs = resp.NewServer(brokerQueue{b}, log)
		go func() {
			if err := rs.ListenAndServe(ctx, *redisAddr); err != nil {
				log.Error("resp server error", "err", err)
			}
		}()
		log.Info("resp server started", "addr", *redisAddr)
	}

	<-ctx.Done()
	log.Info("shutting down...")
	if rs != nil {
		rs.Close()
	}
	if err := b.Stop(); err != nil {
		log.Error("shutdown error", "err", err)
	}
}

// brokerQueue adapts *broker.Broker to the resp.Queue interface.
type brokerQueue struct {
	b *broker.Broker
}

func (a brokerQueue) Enqueue(ctx context.Context, topic, id string, payload json.RawMessage) (string, error) {
	return a.b.Enqueue(ctx, topic, id, payload)
}

func (a brokerQueue) Claim(ctx context.Context, topic, workerID string) (*queue.Job, error) {
	return a.b.Claim(ctx, topic, workerID)
}

func (a brokerQueue) Ack(ctx context.Context, topic, jobID string) error {
	return a.b.Ack(ctx, topic, jobID)
}

func (a brokerQueue) Status(ctx context.Context, topic string) (resp.TopicStatus, error) {
	st, err := a.b.Status(ctx, topic)
	if err != nil {
		return resp.TopicStatus{}, err
	}
	return resp.TopicStatus{
		Pending:   st.Pending,
		Claimed:   st.Claimed,
		Completed: st.Completed,
		Total:     st.Total,
	}, nil
}
