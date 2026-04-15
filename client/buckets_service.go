package client

import (
	"context"
	"fmt"
	"net/url"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

type BucketsService struct {
	requestor request.Requester
}

func NewBucketsService(r request.Requester) *BucketsService {
	return &BucketsService{requestor: r}
}

func (s *BucketsService) List(ctx context.Context) (BucketsResponse, error) {
	var out BucketsResponse
	err := s.requestor.Do(ctx, "GET", common.DataBucketsEndpoint, nil, &out)
	return out, err
}

func (s *BucketsService) Put(ctx context.Context, req PutBucketRequest) error {
	return s.requestor.Do(ctx, "PUT", common.DataBucketsEndpoint, req, nil)
}

func (s *BucketsService) Delete(ctx context.Context, bucket string) error {
	return s.requestor.Do(ctx, "DELETE", fmt.Sprintf(common.DataBucketsRecordsEndpointTemplate, url.PathEscape(bucket)), nil, nil)
}

func (s *BucketsService) AddScope(ctx context.Context, bucket string, req BucketScopeRequest) error {
	return s.requestor.Do(ctx, "POST", fmt.Sprintf(common.DataBucketsScopesEndpointTemplate, url.PathEscape(bucket)), req, nil)
}
