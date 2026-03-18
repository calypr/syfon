package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/drs-server/db/core"
)

func TestRequestIDMiddleware_GeneratesAndPropagates(t *testing.T) {
	m := NewRequestIDMiddleware(nil)
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if core.GetRequestID(r.Context()) == "" {
			t.Fatalf("expected request id in context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get(core.RequestIDHeader) == "" {
		t.Fatalf("expected %s response header", core.RequestIDHeader)
	}
}

func TestRequestIDMiddleware_UsesIncomingHeader(t *testing.T) {
	m := NewRequestIDMiddleware(nil)
	const incoming = "rid-test-123"
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := core.GetRequestID(r.Context()); got != incoming {
			t.Fatalf("expected request id %q in context, got %q", incoming, got)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(core.RequestIDHeader, incoming)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get(core.RequestIDHeader); got != incoming {
		t.Fatalf("expected response header %q, got %q", incoming, got)
	}
}
