package memfs_test

import (
	"context"
	"testing"

	"github.com/vivek-ng/simplequeue/store"
	"github.com/vivek-ng/simplequeue/store/memfs"
)

func TestGetNotFound(t *testing.T) {
	s := memfs.New()
	_, err := s.Get(context.Background(), "missing")
	if err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPutAndGet(t *testing.T) {
	s := memfs.New()
	ctx := context.Background()

	etag, err := s.Put(ctx, "key1", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}

	obj, err := s.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(obj.Data) != "hello" {
		t.Fatalf("expected 'hello', got %q", obj.Data)
	}
	if obj.ETag != etag {
		t.Fatalf("etag mismatch: got %q, want %q", obj.ETag, etag)
	}
}

func TestPutIfCASSuccess(t *testing.T) {
	s := memfs.New()
	ctx := context.Background()

	etag1, _ := s.Put(ctx, "k", []byte("v1"))
	etag2, err := s.PutIf(ctx, "k", []byte("v2"), etag1)
	if err != nil {
		t.Fatal(err)
	}
	if etag2 == etag1 {
		t.Fatal("expected new etag after PutIf")
	}

	obj, _ := s.Get(ctx, "k")
	if string(obj.Data) != "v2" {
		t.Fatalf("expected 'v2', got %q", obj.Data)
	}
}

func TestPutIfCASFailure(t *testing.T) {
	s := memfs.New()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("v1"))
	_, err := s.PutIf(ctx, "k", []byte("v2"), "wrong-etag")
	if err != store.ErrPreconditionFail {
		t.Fatalf("expected ErrPreconditionFail, got %v", err)
	}
}

func TestPutIfCreateNew(t *testing.T) {
	s := memfs.New()
	ctx := context.Background()

	etag, err := s.PutIf(ctx, "new", []byte("data"), "")
	if err != nil {
		t.Fatal(err)
	}
	if etag == "" {
		t.Fatal("expected non-empty etag")
	}

	obj, _ := s.Get(ctx, "new")
	if string(obj.Data) != "data" {
		t.Fatalf("expected 'data', got %q", obj.Data)
	}
}

func TestDelete(t *testing.T) {
	s := memfs.New()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("v"))
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(ctx, "k")
	if err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	s := memfs.New()
	err := s.Delete(context.Background(), "nope")
	if err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	s := memfs.New()
	ctx := context.Background()

	s.Put(ctx, "k", []byte("original"))
	obj, _ := s.Get(ctx, "k")
	obj.Data[0] = 'X' // mutate returned data

	obj2, _ := s.Get(ctx, "k")
	if string(obj2.Data) != "original" {
		t.Fatal("Get did not return a copy; mutation leaked")
	}
}
