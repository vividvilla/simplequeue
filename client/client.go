// Package client provides an SDK for interacting with a simplequeue broker.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/vivek-ng/simplequeue/broker"
	"github.com/vivek-ng/simplequeue/queue"
	"github.com/vivek-ng/simplequeue/store"
)

// Client talks to a simplequeue broker over HTTP.
type Client struct {
	brokerAddr   string
	httpClient   *http.Client
	topic        string
	store        store.ObjectStore
	staleTimeout time.Duration
}

// Option configures a Client.
type Option func(*Client)

func WithHTTPClient(c *http.Client) Option { return func(cl *Client) { cl.httpClient = c } }

// WithTopic sets the topic for all queue operations.
func WithTopic(topic string) Option { return func(cl *Client) { cl.topic = topic } }

// WithDiscovery enables broker discovery via object storage.
func WithDiscovery(s store.ObjectStore, staleTimeout time.Duration) Option {
	return func(cl *Client) {
		cl.store = s
		cl.staleTimeout = staleTimeout
	}
}

// New creates a client pointing at a known broker address.
func New(brokerAddr string, opts ...Option) *Client {
	c := &Client{
		brokerAddr: brokerAddr,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		topic:      "default",
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Discover creates a client that finds the broker via broker.json.
func Discover(ctx context.Context, s store.ObjectStore, staleTimeout time.Duration, opts ...Option) (*Client, error) {
	addr, err := broker.DiscoverBroker(ctx, s, staleTimeout)
	if err != nil {
		return nil, err
	}

	allOpts := append([]Option{WithDiscovery(s, staleTimeout)}, opts...)
	return New(addr, allOpts...), nil
}

// topicPrefix returns the URL path prefix for the configured topic.
func (c *Client) topicPrefix() string {
	return "/topics/" + c.topic
}

type EnqueueRequest struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type EnqueueResponse struct {
	ID string `json:"id"`
}

func (c *Client) Enqueue(ctx context.Context, id string, payload json.RawMessage) (string, error) {
	req := EnqueueRequest{ID: id, Payload: payload}
	resp, err := c.post(ctx, c.topicPrefix()+"/enqueue", req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", c.readError(resp)
	}
	var er EnqueueResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return "", fmt.Errorf("decode enqueue response: %w", err)
	}
	return er.ID, nil
}

type ClaimRequest struct {
	WorkerID string `json:"worker_id"`
}

type ClaimResponse struct {
	Job *queue.Job `json:"job"`
}

func (c *Client) Claim(ctx context.Context, workerID string) (*queue.Job, error) {
	req := ClaimRequest{WorkerID: workerID}
	resp, err := c.post(ctx, c.topicPrefix()+"/claim", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.readError(resp)
	}
	var cr ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}
	return cr.Job, nil
}

func (c *Client) Ack(ctx context.Context, jobID string) error {
	return c.jobIDAction(ctx, c.topicPrefix()+"/ack", jobID)
}

func (c *Client) Nack(ctx context.Context, jobID string) error {
	return c.jobIDAction(ctx, c.topicPrefix()+"/nack", jobID)
}

func (c *Client) Heartbeat(ctx context.Context, jobID string) error {
	return c.jobIDAction(ctx, c.topicPrefix()+"/heartbeat", jobID)
}

func (c *Client) jobIDAction(ctx context.Context, path, jobID string) error {
	req := struct {
		JobID string `json:"job_id"`
	}{JobID: jobID}
	resp, err := c.post(ctx, path, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return c.readError(resp)
	}
	return nil
}

type StatusResponse struct {
	Pending   int `json:"pending"`
	Claimed   int `json:"claimed"`
	Completed int `json:"completed"`
	Total     int `json:"total"`
}

func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	url := c.brokerAddr + c.topicPrefix() + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var sr StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, err
	}
	return &sr, nil
}

func (c *Client) post(ctx context.Context, path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	url := c.brokerAddr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.httpClient.Do(req)
}

func (c *Client) readError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
}
