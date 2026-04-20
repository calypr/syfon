package logs

import (
	"io"
	"log/slog"
)

// NewSlogNoOpLogger creates a no-op slog logger for testing.
func NewSlogNoOpLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
