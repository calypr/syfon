package client

import "context"

type CoreService struct {
	c *Client
}

func (s *CoreService) SHA256Validity(ctx context.Context, values []string) (map[string]bool, error) {
	payload := map[string][]string{"sha256": values}
	out := map[string]bool{}
	err := s.c.doJSON(ctx, "POST", "/index/v1/sha256/validity", nil, payload, &out)
	return out, err
}
