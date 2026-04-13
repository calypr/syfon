package client

import (
	"context"
	"net/url"
)

type BucketsService struct {
	base *baseService
}

func (s *BucketsService) List(ctx context.Context) (BucketsResponse, error) {
	var out BucketsResponse
	rb := s.base.requestor.New("GET", "/data/buckets")
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *BucketsService) Put(ctx context.Context, req PutBucketRequest) error {
	rb, err := s.base.requestor.New("PUT", "/data/buckets").WithJSONBody(req)
	if err != nil {
		return err
	}
	return s.base.requestor.DoJSON(ctx, rb, nil)
}

func (s *BucketsService) Delete(ctx context.Context, bucket string) error {
	rb := s.base.requestor.New("DELETE", "/data/buckets/"+url.PathEscape(bucket))
	return s.base.requestor.DoJSON(ctx, rb, nil)
}

func (s *BucketsService) AddScope(ctx context.Context, bucket string, req BucketScopeRequest) error {
	rb, err := s.base.requestor.New("POST", "/data/buckets/"+url.PathEscape(bucket)+"/scopes").WithJSONBody(req)
	if err != nil {
		return err
	}
	return s.base.requestor.DoJSON(ctx, rb, nil)
}

// --- BucketsService ---
