package buckets

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/requestmeta"
)

func TestAuditCredentialAccessPreservesFields(t *testing.T) {
	orig := slog.Default()
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(orig) })

	ctx := requestmeta.WithRequestID(context.Background(), "req-abc")
	AuditCredentialAccess(ctx, requestmeta.GetRequestID(ctx), "read", "bucket-a", nil)
	AuditCredentialAccess(access.WithSession(ctx, access.NewSession("gen3")), requestmeta.GetRequestID(ctx), "write", "bucket-b", errors.New("boom"))

	out := buf.String()
	for _, want := range []string{"s3 credential audit", "request_id=req-abc", "result=success", "result=error", "mode=gen3"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q in audit output %q", want, out)
		}
	}
}
