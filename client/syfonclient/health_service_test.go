package syfonclient

import (
	"context"
	"errors"
	"testing"

	"github.com/calypr/syfon/client/request"
)

type mockRequester struct {
	doFunc func(ctx context.Context, method, path string, body, out any, opts ...request.RequestOption) error
}

func (m *mockRequester) Do(ctx context.Context, method, path string, body, out any, opts ...request.RequestOption) error {
	if m.doFunc != nil {
		return m.doFunc(ctx, method, path, body, out, opts...)
	}
	return nil
}

func TestHealthService_Ping(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mr := &mockRequester{
			doFunc: func(ctx context.Context, method, path string, body, out any, opts ...request.RequestOption) error {
				if method != "GET" || path != "/healthz" {
					t.Errorf("unexpected request: %s %s", method, path)
				}
				return nil
			},
		}
		h := NewHealthService(mr)
		if err := h.Ping(context.Background()); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("error", func(t *testing.T) {
		mr := &mockRequester{
			doFunc: func(ctx context.Context, method, path string, body, out any, opts ...request.RequestOption) error {
				return errors.New("health check failed")
			},
		}
		h := NewHealthService(mr)
		if err := h.Ping(context.Background()); err == nil {
			t.Error("expected error, got nil")
		}
	})
}
