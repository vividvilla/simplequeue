// Package s3 provides an AWS S3 ObjectStore implementation.
package s3

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	qstore "github.com/vivek-ng/simplequeue/store"
)

// Store implements ObjectStore using AWS S3.
type Store struct {
	client *s3.Client
	bucket string
}

// New creates a new S3-backed store.
func New(client *s3.Client, bucket string) *Store {
	return &Store{client: client, bucket: bucket}
}

func (s *Store) Get(ctx context.Context, key string) (*qstore.Object, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, qstore.ErrNotFound
		}
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}

	return &qstore.Object{Data: data, ETag: etag}, nil
}

func (s *Store) Put(ctx context.Context, key string, data []byte) (string, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", err
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return etag, nil
}

func (s *Store) PutIf(ctx context.Context, key string, data []byte, expectedETag string) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}

	// S3 doesn't have native CAS, but we can use If-Match header via
	// the IfMatch field introduced in recent SDK versions, or we can use
	// conditional writes (S3 supports If-None-Match for new objects).
	// For existing objects, we use the If-Match precondition.
	if expectedETag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(expectedETag)
	}

	out, err := s.client.PutObject(ctx, input)
	if err != nil {
		// S3 returns 412 Precondition Failed for CAS mismatches
		var apiErr interface{ HTTPStatusCode() int }
		if errors.As(err, &apiErr) && apiErr.HTTPStatusCode() == 412 {
			return "", qstore.ErrPreconditionFail
		}
		return "", err
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return etag, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	return err
}
