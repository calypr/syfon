package common

import (
	"context"
	"log/slog"
)

// AuditS3CredentialAccess logs credential access events with request/mode context.
func AuditS3CredentialAccess(ctx context.Context, action string, bucket string, err error) {
	requestID := GetRequestID(ctx)
	mode := "local"
	if v, ok := ctx.Value(AuthModeKey).(string); ok && v == "gen3" {
		mode = "gen3"
	}
	if err != nil {
		slog.Warn("s3 credential audit", "action", action, "bucket", bucket, "request_id", requestID, "mode", mode, "result", "error", "err", err)
		return
	}
	slog.Info("s3 credential audit", "action", action, "bucket", bucket, "request_id", requestID, "mode", mode, "result", "success")
}
