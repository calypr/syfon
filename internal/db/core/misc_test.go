package core

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
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
	obj := InternalObject{DrsObject: drs.DrsObject{Id: "obj-1", Name: Ptr("n")}}
	ext := obj.External()
	if ext.Id != "obj-1" || StringVal(ext.Name) != "n" {
		t.Fatalf("unexpected external object: %+v", ext)
	}
}

func TestCredentialEncryptionEnabled(t *testing.T) {
	t.Setenv(CredentialMasterKeyEnv, "")
	t.Setenv(CredentialLocalKeyFileEnv, "")
	t.Setenv(DatabaseSQLiteFileEnv, t.TempDir()+"/drs.db")

	enabled, err := CredentialEncryptionEnabled()
	if err != nil {
		t.Fatalf("CredentialEncryptionEnabled returned error: %v", err)
	}
	if !enabled {
		t.Fatal("expected credential encryption to be enabled")
	}
}

func TestCredentialEncryptionEnabled_InvalidProvidedKey(t *testing.T) {
	t.Setenv(CredentialMasterKeyEnv, "not-a-valid-key")

	enabled, err := CredentialEncryptionEnabled()
	if err == nil {
		t.Fatal("expected error for invalid provided key")
	}
	if enabled {
		t.Fatal("expected encryption to be disabled on key parse error")
	}
}

func TestParseUserProvidedKey_FormatsAndErrors(t *testing.T) {
	b64 := "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	key, err := parseUserProvidedKey(b64, "X")
	if err != nil || len(key) != 32 {
		t.Fatalf("expected valid base64 32-byte key, err=%v len=%d", err, len(key))
	}

	raw := "raw-key-material-32-bytes-long!!"
	key, err = parseUserProvidedKey(raw, "X")
	if err != nil || len(key) != 32 {
		t.Fatalf("expected valid raw 32-byte key, err=%v len=%d", err, len(key))
	}

	if _, err := parseUserProvidedKey("short", "X"); err == nil {
		t.Fatal("expected invalid key parse error")
	}
}

func TestResolveAndRegisterCredentialKeyManager(t *testing.T) {
	if err := RegisterCredentialKeyManager("", func() (CredentialKeyManager, error) { return &localKeyManager{}, nil }); err == nil {
		t.Fatal("expected error when registering empty manager name")
	}
	if err := RegisterCredentialKeyManager("bad", nil); err == nil {
		t.Fatal("expected error when registering nil factory")
	}

	if _, err := resolveCredentialKeyManager("definitely-missing"); err == nil {
		t.Fatal("expected error for unknown key manager")
	}

	const name = "test-nil-manager"
	if err := RegisterCredentialKeyManager(name, func() (CredentialKeyManager, error) { return nil, nil }); err != nil {
		t.Fatalf("register manager failed: %v", err)
	}
	if _, err := resolveCredentialKeyManager(name); err == nil {
		t.Fatal("expected error when manager factory returns nil")
	}
}

func TestLocalCredentialKeyPathAndLoadErrors(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv(CredentialLocalKeyFileEnv, tmp+"/kek")
	t.Setenv(DatabaseSQLiteFileEnv, "")
	if got := localCredentialKeyPath(); got != tmp+"/kek" {
		t.Fatalf("unexpected local key path: %q", got)
	}

	invalid := tmp + "/invalid-kek"
	if err := os.WriteFile(invalid, []byte("bad-key-material\n"), 0o600); err != nil {
		t.Fatalf("failed to seed invalid key file: %v", err)
	}
	t.Setenv(CredentialLocalKeyFileEnv, invalid)
	if _, err := loadOrCreateLocalCredentialKey(); err == nil {
		t.Fatal("expected invalid local key file parse error")
	}
}
