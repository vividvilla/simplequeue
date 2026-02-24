// Package memfs provides an in-memory ObjectStore for testing.
package memfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/vivek-ng/simplequeue/store"
)

type entry struct {
	data []byte
	etag string
}

// Store is a thread-safe in-memory ObjectStore.
type Store struct {
	mu      sync.Mutex
	objects map[string]entry
	counter uint64
}

// New returns a new in-memory store.
func New() *Store {
	return &Store{objects: make(map[string]entry)}
}

func (s *Store) etag(data []byte) string {
	s.counter++
	h := sha256.Sum256(append(data, []byte(fmt.Sprintf("%d", s.counter))...))
	return fmt.Sprintf("%x", h[:8])
}

func (s *Store) Get(_ context.Context, key string) (*store.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.objects[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := make([]byte, len(e.data))
	copy(cp, e.data)
	return &store.Object{Data: cp, ETag: e.etag}, nil
}

func (s *Store) Put(_ context.Context, key string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]byte, len(data))
	copy(cp, data)
	tag := s.etag(data)
	s.objects[key] = entry{data: cp, etag: tag}
	return tag, nil
}

func (s *Store) PutIf(_ context.Context, key string, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.objects[key]
	if !ok {
		// If expectedETag is empty, allow creation of a new object.
		if expectedETag == "" {
			cp := make([]byte, len(data))
			copy(cp, data)
			tag := s.etag(data)
			s.objects[key] = entry{data: cp, etag: tag}
			return tag, nil
		}
		return "", store.ErrNotFound
	}

	if e.etag != expectedETag {
		return "", store.ErrPreconditionFail
	}

	cp := make([]byte, len(data))
	copy(cp, data)
	tag := s.etag(data)
	s.objects[key] = entry{data: cp, etag: tag}
	return tag, nil
}

func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.objects[key]; !ok {
		return store.ErrNotFound
	}
	delete(s.objects, key)
	return nil
}
