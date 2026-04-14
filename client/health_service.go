package client

import "context"

type HealthService struct {
	base *baseService
}

func (h *HealthService) Ping(ctx context.Context) error {
	rb := h.base.requestor.New("GET", "/healthz")
	return h.base.requestor.DoJSON(ctx, rb, nil)
}

// --- HealthService ---
