// Package localfs provides a local-filesystem ObjectStore for durable single-node usage.
package localfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/vivek-ng/simplequeue/store"
)

// Store is a file-backed ObjectStore that persists data to a local directory.
type Store struct {
	dir     string
	mu      sync.Mutex
	counter uint64
}

// New returns a new local filesystem store rooted at dir.
// The directory is created (with parents) if it does not exist.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("localfs: create dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(key string) string     { return filepath.Join(s.dir, key) }
func (s *Store) etagPath(key string) string  { return filepath.Join(s.dir, key+".etag") }

func (s *Store) genETag(data []byte) string {
	s.counter++
	h := sha256.Sum256(append(data, []byte(fmt.Sprintf("%d", s.counter))...))
	return fmt.Sprintf("%x", h[:8])
}

// atomicWrite writes data to path via a temp file + rename for crash safety.
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *Store) Get(_ context.Context, key string) (*store.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path(key))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}

	etag, err := os.ReadFile(s.etagPath(key))
	if err != nil {
		return nil, err
	}

	return &store.Object{Data: data, ETag: string(etag)}, nil
}

func (s *Store) Put(_ context.Context, key string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := atomicWrite(s.path(key), data); err != nil {
		return "", err
	}

	tag := s.genETag(data)
	if err := atomicWrite(s.etagPath(key), []byte(tag)); err != nil {
		return "", err
	}

	return tag, nil
}

func (s *Store) PutIf(_ context.Context, key string, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentETag, err := os.ReadFile(s.etagPath(key))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		// File doesn't exist — allow creation only if expectedETag is empty.
		if expectedETag != "" {
			return "", store.ErrNotFound
		}
	} else {
		if string(currentETag) != expectedETag {
			return "", store.ErrPreconditionFail
		}
	}

	if err := atomicWrite(s.path(key), data); err != nil {
		return "", err
	}

	tag := s.genETag(data)
	if err := atomicWrite(s.etagPath(key), []byte(tag)); err != nil {
		return "", err
	}

	return tag, nil
}

func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := os.Stat(s.path(key)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return store.ErrNotFound
		}
		return err
	}

	os.Remove(s.etagPath(key))
	return os.Remove(s.path(key))
}
