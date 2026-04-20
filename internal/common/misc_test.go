package common

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/models"
)

func TestWithAndGetRequestID(t *testing.T) {
	ctx := context.Background()
	if got := GetRequestID(ctx); got != "" {
		t.Fatalf("expected empty request id, got %q", got)
	}

	ctx = WithRequestID(ctx, "rid-123")
	if got := GetRequestID(ctx); got != "rid-123" {
		t.Fatalf("expected request id rid-123, got %q", got)
	}
}

func TestAuditS3CredentialAccess_LogsSuccessAndError(t *testing.T) {
	orig := slog.Default()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(orig) })

	ctx := WithRequestID(context.Background(), "req-abc")
	AuditS3CredentialAccess(ctx, "read", "bucket-a", nil)

	errCtx := context.WithValue(ctx, AuthModeKey, "gen3")
	AuditS3CredentialAccess(errCtx, "write", "bucket-b", errors.New("boom"))

	out := buf.String()
	if !strings.Contains(out, "s3 credential audit") || !strings.Contains(out, "request_id=req-abc") {
		t.Fatalf("expected audit log with request id, got %q", out)
	}
	if !strings.Contains(out, "result=success") {
		t.Fatalf("expected success audit log, got %q", out)
	}
	if !strings.Contains(out, "result=error") || !strings.Contains(out, "mode=gen3") {
		t.Fatalf("expected gen3 error audit log, got %q", out)
	}
}

func TestInternalObjectExternal(t *testing.T) {
	obj := models.InternalObject{DrsObject: drs.DrsObject{Id: "obj-1", Name: Ptr("n")}}
	ext := obj.External()
	if ext.Id != "obj-1" || StringVal(ext.Name) != "n" {
		t.Fatalf("unexpected external object: %+v", ext)
	}
}

func TestInternalObjectJSONAliases(t *testing.T) {
	raw := []byte(`{"did":"obj-1","size":7,"authz":["/programs/test"],"hashes":{"sha256":"abc"},"urls":["https://example.org/file"],"extra":"keep-me"}`)

	var obj models.InternalObject
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if obj.Id != "obj-1" {
		t.Fatalf("expected did to map to id, got %q", obj.Id)
	}
	if obj.Size != 7 {
		t.Fatalf("expected size 7, got %d", obj.Size)
	}
	if len(obj.Authorizations) != 1 || obj.Authorizations[0] != "/programs/test" {
		t.Fatalf("expected authz to map, got %v", obj.Authorizations)
	}
	if len(obj.Checksums) != 1 || obj.Checksums[0].Type != "sha256" || obj.Checksums[0].Checksum != "abc" {
		t.Fatalf("expected hashes to map to checksums, got %+v", obj.Checksums)
	}
	if obj.AccessMethods == nil || len(*obj.AccessMethods) != 1 {
		t.Fatalf("expected urls to map to access methods, got %+v", obj.AccessMethods)
	}
	if got := (*obj.AccessMethods)[0].AccessUrl; got == nil || got.Url != "https://example.org/file" {
		t.Fatalf("expected mapped access url, got %+v", got)
	}

	out, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var roundTripped map[string]any
	if err := json.Unmarshal(out, &roundTripped); err != nil {
		t.Fatalf("decode marshaled payload: %v", err)
	}
	if got := roundTripped["did"]; got != "obj-1" {
		t.Fatalf("expected did alias in output, got %v", got)
	}
	if got := roundTripped["id"]; got != "obj-1" {
		t.Fatalf("expected id in output, got %v", got)
	}
	if got := roundTripped["extra"]; got != "keep-me" {
		t.Fatalf("expected unknown fields to survive, got %v", got)
	}
}
