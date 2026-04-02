package client

import (
	"context"
	"net/url"
)

type BucketsService struct {
	c *Client
}

func (s *BucketsService) List(ctx context.Context) (BucketsResponse, error) {
	var out BucketsResponse
	err := s.c.doJSON(ctx, "GET", "/data/buckets", nil, nil, &out)
	return out, err
}

func (s *BucketsService) Put(ctx context.Context, req PutBucketRequest) error {
	return s.c.doJSON(ctx, "PUT", "/data/buckets", nil, req, nil)
}

func (s *BucketsService) Delete(ctx context.Context, bucket string) error {
	return s.c.doJSON(ctx, "DELETE", "/data/buckets/"+url.PathEscape(bucket), nil, nil, nil)
}

func (s *BucketsService) AddScope(ctx context.Context, bucket string, req BucketScopeRequest) error {
	return s.c.doJSON(ctx, "POST", "/data/buckets/"+url.PathEscape(bucket)+"/scopes", nil, req, nil)
}

// Compatibility wrapper used by current CLI code.
func (c *Client) PutBucket(ctx context.Context, req PutBucketRequest) error {
	return c.Buckets().Put(ctx, req)
}
