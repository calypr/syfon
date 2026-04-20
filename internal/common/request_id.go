package common

import "context"

const RequestIDHeader = "X-Request-Id"

const RequestIDKey AuthzContextKey = "request_id"

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDKey, requestID)
}

func GetRequestID(ctx context.Context) string {
	v := ctx.Value(RequestIDKey)
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
