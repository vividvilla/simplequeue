// Package store defines the abstract storage interface for the queue backend.
package store

import (
	"context"
	"errors"
)

var (
	ErrNotFound         = errors.New("object not found")
	ErrPreconditionFail = errors.New("precondition failed: ETag mismatch")
)

// Object represents a stored object with its data and version tag.
type Object struct {
	Data []byte
	ETag string
}

// ObjectStore is the interface that queue backends must implement.
// ETags are opaque version strings used for compare-and-set operations.
type ObjectStore interface {
	Get(ctx context.Context, key string) (*Object, error)
	Put(ctx context.Context, key string, data []byte) (newETag string, err error)
	PutIf(ctx context.Context, key string, data []byte, expectedETag string) (newETag string, err error)
	Delete(ctx context.Context, key string) error
}
