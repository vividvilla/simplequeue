# simplequeue

A distributed job queue built on object storage. Uses compare-and-set (CAS) operations for coordination instead of distributed locks, so the only infrastructure you need is a storage backend (local filesystem or S3).

Inspired by - https://turbopuffer.com/blog/object-storage-queue

## Features

- **Object storage backed** — queue state lives in any object storage like S3 (or local filesystem), no database required
- **CAS-based coordination** — no distributed locks; all mutations use optimistic concurrency with ETags
- **Redis protocol compatible** — use `redis-cli` or any Redis client library to enqueue and consume jobs via RESP2 (LIST and STREAM commands)
- **HTTP API** — REST endpoints for enqueue, claim, ack, nack, heartbeat, and status
- **Group commit** — concurrent mutations are batched into single CAS writes to reduce contention
- **Stale job reaping** — claimed jobs that miss heartbeats are automatically returned to pending
- **Lazy topics** — topics are created on first use, no upfront configuration needed
- **Broker discovery** — clients find the active broker via `broker.json` in the storage backend

## Quick start

```bash
make build
```

### Using the CLI

```bash
# Start the broker with local filesystem storage + Redis interface
./bin/simplequeued -backend localfs -dir /tmp/sq-data

# Enqueue a job (ID auto-generated)
./bin/simplequeue -topic orders enqueue '{"item":"book"}'

# Or with an explicit ID
./bin/simplequeue -topic orders enqueue --id job-1 '{"item":"book"}'

# Claim and process
./bin/simplequeue -topic orders claim worker-1
./bin/simplequeue -topic orders ack job-1

# Check status
./bin/simplequeue -topic orders status
```

### Using redis-cli

```bash
# Start the broker with local filesystem storage + Redis interface
./bin/simplequeued -backend localfs -dir /tmp/sq-data -redis-addr :6379

# Enqueue jobs
redis-cli LPUSH orders '{"item":"book"}'
redis-cli RPUSH orders '{"item":"pen"}' '{"item":"notebook"}'

# Check pending count
redis-cli LLEN orders

# Claim and consume
redis-cli RPOP orders

# Stream-style: enqueue with field-value pairs
redis-cli XADD events '*' action click user alice

# Consume as a named worker
redis-cli XREADGROUP GROUP default worker1 COUNT 1 STREAMS events '>'

# Acknowledge processing
redis-cli XACK events default <job-id>
```

## Architecture

- **Broker** (`simplequeued`) — stateless HTTP server that reads/writes queue state to object storage. Supports multiple independent topics, each stored in its own `{topic}.json` file. Broker metadata lives in `broker.json`. Optionally exposes a Redis-compatible RESP2 TCP listener.
- **Client** (`simplequeue`) — CLI and Go SDK for enqueuing, claiming, and managing jobs.
- **Storage backends** — in-memory (testing), local filesystem (single-node), AWS S3 (distributed).

## Redis-compatible interface

The broker exposes a RESP2 TCP listener when started with `-redis-addr`. Any Redis client library or `redis-cli` works out of the box.

### Supported commands

| Command | Description |
|---------|-------------|
| `LPUSH key value [value...]` | Enqueue values to topic |
| `RPUSH key value [value...]` | Same as LPUSH |
| `RPOP key` | Claim next pending job, return payload |
| `LPOP key` | Same as RPOP |
| `LLEN key` | Pending job count |
| `XADD key * field value [...]` | Enqueue field-value pairs as JSON |
| `XREADGROUP GROUP g consumer COUNT n STREAMS key >` | Claim jobs as a named consumer |
| `XACK key group id [id...]` | Acknowledge completed jobs |
| `XLEN key` | Total job count |
| `XINFO STREAM key` | Topic stats (length, pending, claimed, completed) |
| `PING` | Returns PONG |
| `QUIT` | Close connection |
| `COMMAND` | Returns empty array (keeps redis-cli happy) |

## HTTP API

All job operations are scoped to a topic via `/topics/{topic}/...`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/topics/{topic}/enqueue` | Add a job (`{"id": "...", "payload": {...}}`). `id` is optional — auto-generated if omitted. |
| `POST` | `/topics/{topic}/claim` | Claim next pending job (`{"worker_id": "..."}`) |
| `POST` | `/topics/{topic}/ack` | Mark job completed (`{"job_id": "..."}`) |
| `POST` | `/topics/{topic}/nack` | Return job to pending (`{"job_id": "..."}`) |
| `POST` | `/topics/{topic}/heartbeat` | Update job heartbeat (`{"job_id": "..."}`) |
| `GET` | `/topics/{topic}/status` | Queue stats (pending/claimed/completed/total) |
| `GET` | `/healthz` | Health check |

Topic names must match `[a-zA-Z0-9_-]+`.

## Broker flags

```
simplequeued [flags]

  -addr string        listen address (default ":8080")
  -redis-addr string  Redis-compatible listener address (e.g. :6379), disabled if empty
  -broker-id string   unique broker identifier (default: addr)
  -backend string     storage backend: memfs, localfs, or s3 (default "memfs")
  -dir string         storage directory (required for localfs)
  -bucket string      S3 bucket name (required for s3)
```

## CLI flags

```
simplequeue [flags] <command> [args...]

  -broker string   broker address (default "http://localhost:8080")
  -topic string    topic name (default "default")

commands:
  enqueue [--id <id>] <payload-json>   (ID auto-generated if omitted)
  claim <worker-id>
  ack <job-id>
  nack <job-id>
  heartbeat <job-id>
  status
```

## Go SDK

```go
import "github.com/vivek-ng/simplequeue/client"

c := client.New("http://localhost:8080", client.WithTopic("orders"))

// Enqueue (ID auto-generated if empty)
id, _ := c.Enqueue(ctx, "", json.RawMessage(`{"item":"book"}`))

// Or with an explicit ID
id, _ = c.Enqueue(ctx, "job-1", json.RawMessage(`{"item":"book"}`))

// Claim
job, _ := c.Claim(ctx, "worker-1")

// Ack / Nack
c.Ack(ctx, job.ID)
c.Nack(ctx, job.ID)

// Heartbeat (call periodically for long-running jobs)
c.Heartbeat(ctx, job.ID)

// Status
status, _ := c.Status(ctx)
```

### Broker discovery

Clients can discover the broker address from storage instead of hardcoding it:

```go
c, err := client.Discover(ctx, store, 30*time.Second, client.WithTopic("orders"))
```

## Storage backends

| Backend | Use case | Flag |
|---------|----------|------|
| `memfs` | Testing, ephemeral queues | `-backend memfs` |
| `localfs` | Single-node, durable | `-backend localfs -dir /path/to/data` |
| `s3` | Distributed, multi-node | `-backend s3 -bucket my-bucket` |

All backends implement the `ObjectStore` interface with `Get`, `Put`, `PutIf` (CAS), and `Delete`.

### S3 example

Run a distributed queue backed by S3. Each topic is stored as a single `{topic}.json` object in the bucket, and coordination happens via S3's conditional writes (ETags).

```bash
# Create a bucket
aws s3 mb s3://my-queue-data

# Start the broker
./bin/simplequeued -backend s3 -bucket my-queue-data -addr :8080

# Enqueue and process from any machine that can reach the broker
./bin/simplequeue -broker http://broker-host:8080 -topic orders enqueue '{"item":"book"}'
./bin/simplequeue -broker http://broker-host:8080 -topic orders claim worker-1
```

Multiple brokers can run against the same bucket for high availability — CAS ensures only one write succeeds per round, and the group committer retries automatically on conflicts.

```bash
# Node A
./bin/simplequeued -backend s3 -bucket my-queue-data -addr :8080 -broker-id node-a

# Node B
./bin/simplequeued -backend s3 -bucket my-queue-data -addr :8081 -broker-id node-b
```

Clients discover the active broker via `broker.json` in the bucket:

```go
s3Client := s3.NewFromConfig(awsCfg)
store := s3store.New(s3Client, "my-queue-data")
c, err := client.Discover(ctx, store, 30*time.Second, client.WithTopic("orders"))
```

## Development

```bash
make build       # compile binaries to bin/
make test        # run tests
make test-race   # run tests with race detector
make lint        # go vet
make clean       # remove bin/
```
