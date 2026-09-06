package s3

import (
	"context"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/storage"
)

type credentialLookupFunc func(context.Context, string) (*buckets.Credential, error)

func (f credentialLookupFunc) GetS3Credential(ctx context.Context, bucket string) (*buckets.Credential, error) {
	return f(ctx, bucket)
}

func TestNewS3SignerAcceptsCredentialLookupOnly(t *testing.T) {
	var _ storage.CredentialLookup = credentialLookupFunc(nil)

	signer := NewS3Signer(credentialLookupFunc(func(context.Context, string) (*buckets.Credential, error) {
		return &buckets.Credential{
			Region:    "us-east-1",
			AccessKey: "access-key",
			SecretKey: "secret-key",
			Endpoint:  "http://127.0.0.1:1",
		}, nil
	}))
	if _, err := signer.getClients(context.Background(), "bucket"); err != nil {
		t.Fatalf("getClients with one-method lookup failed: %v", err)
	}
}
