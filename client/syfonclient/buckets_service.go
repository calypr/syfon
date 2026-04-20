package syfonclient

import (
	"context"
	"fmt"
	"net/http"

	"github.com/calypr/syfon/apigen/client/bucketapi"
)

type BucketsService struct {
	gen bucketapi.ClientWithResponsesInterface
}

func NewBucketsService(gen bucketapi.ClientWithResponsesInterface) *BucketsService {
	return &BucketsService{gen: gen}
}

func (s *BucketsService) List(ctx context.Context) (bucketapi.BucketsResponse, error) {
	resp, err := s.gen.ListBucketsWithResponse(ctx)
	if err != nil {
		return bucketapi.BucketsResponse{}, err
	}
	if resp.JSON200 == nil {
		return bucketapi.BucketsResponse{}, fmt.Errorf("unexpected response: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *BucketsService) Put(ctx context.Context, req bucketapi.PutBucketRequest) error {
	resp, err := s.gen.PutBucketWithResponse(ctx, bucketapi.PutBucketJSONRequestBody(req))
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("failed to put bucket: %d", resp.StatusCode())
	}
	return nil
}

func (s *BucketsService) Delete(ctx context.Context, bucket string) error {
	resp, err := s.gen.DeleteBucketWithResponse(ctx, bucket)
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("failed to delete bucket: %d", resp.StatusCode())
	}
	return nil
}

func (s *BucketsService) AddScope(ctx context.Context, bucket string, req bucketapi.AddBucketScopeRequest) error {
	resp, err := s.gen.AddBucketScopeWithResponse(ctx, bucket, bucketapi.AddBucketScopeJSONRequestBody(req))
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("failed to add scope: %d", resp.StatusCode())
	}
	return nil
}
