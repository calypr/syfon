package middleware

import (
	"github.com/calypr/syfon/internal/common"

	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
)

func TestRequestIDMiddleware_GeneratesAndPropagates(t *testing.T) {
	m := NewRequestIDMiddleware(nil)
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		if common.GetRequestID(c.Context()) == "" {
			t.Fatalf("expected request id in context")
		}
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get(common.RequestIDHeader) == "" {
		t.Fatalf("expected %s response header", common.RequestIDHeader)
	}
}

func TestRequestIDMiddleware_UsesIncomingHeader(t *testing.T) {
	m := NewRequestIDMiddleware(nil)
	const incoming = "rid-test-123"
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		if got := common.GetRequestID(c.Context()); got != incoming {
			t.Fatalf("expected request id %q in context, got %q", incoming, got)
		}
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(common.RequestIDHeader, incoming)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}

	if got := resp.Header.Get(common.RequestIDHeader); got != incoming {
		t.Fatalf("expected response header %q, got %q", incoming, got)
	}
}
