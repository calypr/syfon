package gcs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/signer"
	"github.com/calypr/syfon/internal/storage"
)

type credentialLookupFunc func(context.Context, string) (*buckets.Credential, error)

func (f credentialLookupFunc) GetS3Credential(ctx context.Context, bucket string) (*buckets.Credential, error) {
	return f(ctx, bucket)
}

func TestGCSSignerCredentialLookupPort(t *testing.T) {
	var _ storage.CredentialLookup = credentialLookupFunc(nil)

	t.Run("accepts one-method lookup", func(t *testing.T) {
		gcsSigner := NewGCSSigner(credentialLookupFunc(func(context.Context, string) (*buckets.Credential, error) {
			return &buckets.Credential{Endpoint: "http://localhost:4443"}, nil
		}))
		if _, err := gcsSigner.SignURL(context.Background(), "bucket", "object", signer.SignOptions{}); err != nil {
			t.Fatalf("SignURL with one-method lookup failed: %v", err)
		}
	})

	t.Run("preserves lookup error", func(t *testing.T) {
		wantErr := errors.New("lookup failed")
		gcsSigner := NewGCSSigner(credentialLookupFunc(func(context.Context, string) (*buckets.Credential, error) {
			return nil, wantErr
		}))
		_, err := gcsSigner.SignURL(context.Background(), "bucket", "object", signer.SignOptions{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("SignURL error = %v, want %v", err, wantErr)
		}
	})

	t.Run("preserves nil credential error", func(t *testing.T) {
		gcsSigner := NewGCSSigner(credentialLookupFunc(func(context.Context, string) (*buckets.Credential, error) {
			return nil, nil
		}))
		_, err := gcsSigner.SignURL(context.Background(), "bucket", "object", signer.SignOptions{})
		if err == nil || !strings.Contains(err.Error(), "credentials not found for bucket bucket") {
			t.Fatalf("SignURL error = %v, want missing-credential error", err)
		}
	})
}
