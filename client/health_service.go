package client

import (
	"context"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

type HealthService struct {
	requestor request.Requester
}

func NewHealthService(r request.Requester) *HealthService {
	return &HealthService{requestor: r}
}

func (h *HealthService) Ping(ctx context.Context) error {
	return h.requestor.Do(ctx, "GET", common.HealthzEndpoint, nil, nil)
}

// --- HealthService ---
