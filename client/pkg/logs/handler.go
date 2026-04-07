package logs

import (
	"context"
	"log/slog"

	"github.com/calypr/syfon/client/pkg/common"
)

// ProgressHandler is a slog.Handler that captures log messages and
// forwards them to a ProgressCallback if one is present in the context.
type ProgressHandler struct {
	next slog.Handler
}

func NewProgressHandler(next slog.Handler) *ProgressHandler {
	if next == nil {
		next = slog.Default().Handler()
	}
	return &ProgressHandler{next: next}
}

func (h *ProgressHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *ProgressHandler) Handle(ctx context.Context, r slog.Record) error {
	// Call the next handler in the chain (original logging)
	err := h.next.Handle(ctx, r)

	// In addition, try to bubble up to progress callback
	cb := common.GetProgress(ctx)
	if cb != nil {
		oid := common.GetOid(ctx)
		// We send an event of type "log"
		attrs := make(map[string]any)
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.Any()
			return true
		})
		_ = cb(common.ProgressEvent{
			Event:   "log",
			Oid:     oid,
			Message: r.Message,
			Level:   r.Level.String(),
			Attrs:   attrs,
		})
	}

	return err
}

func (h *ProgressHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ProgressHandler{next: h.next.WithAttrs(attrs)}
}

func (h *ProgressHandler) WithGroup(name string) slog.Handler {
	return &ProgressHandler{next: h.next.WithGroup(name)}
}

// TeeHandler fans out log records to multiple handlers
type TeeHandler struct {
	handlers []slog.Handler
}

func NewTeeHandler(handlers ...slog.Handler) slog.Handler {
	return &TeeHandler{handlers: handlers}
}

func (h *TeeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, hand := range h.handlers {
		if hand.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *TeeHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, hand := range h.handlers {
		if hand.Enabled(ctx, r.Level) {
			_ = hand.Handle(ctx, r)
		}
	}
	return nil
}

func (h *TeeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandlers := make([]slog.Handler, len(h.handlers))
	for i, hand := range h.handlers {
		newHandlers[i] = hand.WithAttrs(attrs)
	}
	return &TeeHandler{handlers: newHandlers}
}

func (h *TeeHandler) WithGroup(name string) slog.Handler {
	newHandlers := make([]slog.Handler, len(h.handlers))
	for i, hand := range h.handlers {
		newHandlers[i] = hand.WithGroup(name)
	}
	return &TeeHandler{handlers: newHandlers}
}
