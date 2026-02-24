// Package retry provides exponential backoff with jitter.
package retry

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Config controls retry behavior.
type Config struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	MaxRetries int // 0 means unlimited
}

// DefaultConfig returns sensible defaults for CAS retries against object storage.
func DefaultConfig() Config {
	return Config{
		BaseDelay:  50 * time.Millisecond,
		MaxDelay:   2 * time.Second,
		MaxRetries: 10,
	}
}

// Do retries fn until it returns nil, a non-retryable error, or retries are exhausted.
// fn receives the attempt number (0-indexed). If fn returns nil, Do returns nil.
// If shouldRetry is nil, all errors are retried.
func Do(ctx context.Context, cfg Config, shouldRetry func(error) bool, fn func(attempt int) error) error {
	for attempt := 0; ; attempt++ {
		err := fn(attempt)
		if err == nil {
			return nil
		}

		if shouldRetry != nil && !shouldRetry(err) {
			return err
		}

		if cfg.MaxRetries > 0 && attempt >= cfg.MaxRetries-1 {
			return err
		}

		delay := backoff(cfg, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func backoff(cfg Config, attempt int) time.Duration {
	delay := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(cfg.MaxDelay) {
		delay = float64(cfg.MaxDelay)
	}
	// Full jitter: [0, delay)
	jittered := time.Duration(rand.Float64() * delay)
	return jittered
}
