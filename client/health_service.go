package client

import "context"

type HealthService struct {
	c *Client
}

func (h *HealthService) Ping(ctx context.Context) error {
	return h.c.doJSON(ctx, "GET", "/healthz", nil, nil, nil)
}

// Compatibility wrapper.
func (c *Client) Ping(ctx context.Context) error { return c.Health().Ping(ctx) }
