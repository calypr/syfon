package common

import "context"

type ProgressEvent struct {
	Event          string         `json:"event"`
	Oid            string         `json:"oid"`
	BytesSoFar     int64          `json:"bytesSoFar"`
	BytesSinceLast int64          `json:"bytesSinceLast"`
	Message        string         `json:"message,omitempty"`
	Level          string         `json:"level,omitempty"`
	Attrs          map[string]any `json:"attrs,omitempty"`
}

type ProgressCallback func(ProgressEvent) error

type TransferCompletionEvent struct {
	Direction  string
	GUID       string
	RangeStart int64
	RangeEnd   int64
	Bytes      int64
	PartNumber int
	Strategy   string
}

type TransferCompletionCallback func(TransferCompletionEvent) error

type contextKey string

const (
	progressKey           contextKey = "progressCallback"
	oidKey                contextKey = "activeOid"
	transferCompletionKey contextKey = "transferCompletionCallback"
)

func WithProgress(ctx context.Context, cb ProgressCallback) context.Context {
	return context.WithValue(ctx, progressKey, cb)
}

func GetProgress(ctx context.Context) ProgressCallback {
	if cb, ok := ctx.Value(progressKey).(ProgressCallback); ok {
		return cb
	}
	return nil
}

func WithOid(ctx context.Context, oid string) context.Context {
	return context.WithValue(ctx, oidKey, oid)
}

func GetOid(ctx context.Context) string {
	if oid, ok := ctx.Value(oidKey).(string); ok {
		return oid
	}
	return ""
}

func WithTransferCompletion(ctx context.Context, cb TransferCompletionCallback) context.Context {
	return context.WithValue(ctx, transferCompletionKey, cb)
}

func GetTransferCompletion(ctx context.Context) TransferCompletionCallback {
	if cb, ok := ctx.Value(transferCompletionKey).(TransferCompletionCallback); ok {
		return cb
	}
	return nil
}
