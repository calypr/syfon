package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/drs-server/db/core"
)

type RequestIDMiddleware struct {
	logger *slog.Logger
}

func NewRequestIDMiddleware(logger *slog.Logger) *RequestIDMiddleware {
	if logger == nil {
		logger = slog.Default()
	}
	return &RequestIDMiddleware{logger: logger}
}

type requestIDStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *requestIDStatusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (m *RequestIDMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get(core.RequestIDHeader))
		if requestID == "" {
			requestID = newRequestID()
		}

		ctx := core.WithRequestID(r.Context(), requestID)
		r = r.WithContext(ctx)
		r.Header.Set(core.RequestIDHeader, requestID)

		w.Header().Set(core.RequestIDHeader, requestID)
		start := time.Now()
		m.logger.Debug("request start", "request_id", requestID, "method", r.Method, "path", r.URL.Path)

		rec := &requestIDStatusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		m.logger.Debug(
			fmt.Sprintf("[%d] %s %s", rec.status, r.Method, r.URL.Path),
			"request_id", requestID,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func newRequestID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "rid-fallback"
	}
	return hex.EncodeToString(b[:])
}
