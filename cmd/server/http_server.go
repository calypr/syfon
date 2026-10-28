package server

import (
	"context"
	"net/http"
	"sync"
)

type requestTracker struct {
	mu       sync.Mutex
	active   int
	stopping bool
	drained  chan struct{}
}

func newRequestTracker() *requestTracker {
	drained := make(chan struct{})
	close(drained)
	return &requestTracker{drained: drained}
}

func (t *requestTracker) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !t.start() {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		defer t.finish()
		next.ServeHTTP(w, r)
	})
}

func (t *requestTracker) start() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopping {
		return false
	}
	if t.active == 0 {
		t.drained = make(chan struct{})
	}
	t.active++
	return true
}

func (t *requestTracker) finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.active--
	if t.active == 0 {
		close(t.drained)
	}
}

func (t *requestTracker) stopAndWait(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	t.stopping = true
	drained := t.drained
	t.mu.Unlock()
	select {
	case <-drained:
		return true
	case <-ctx.Done():
		select {
		case <-drained:
			return true
		default:
			return false
		}
	}
}
