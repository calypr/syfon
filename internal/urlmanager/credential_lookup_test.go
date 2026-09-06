package urlmanager

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/storage"
)

type credentialLookupFunc func(context.Context, string) (*buckets.Credential, error)

func (f credentialLookupFunc) GetS3Credential(ctx context.Context, bucket string) (*buckets.Credential, error) {
	return f(ctx, bucket)
}

func TestCredentialLookupPort(t *testing.T) {
	var _ storage.CredentialLookup = credentialLookupFunc(nil)

	wantErr := errors.New("lookup failed")
	cred := &buckets.Credential{Bucket: "bucket", Provider: "s3"}
	tests := []struct {
		name     string
		bucket   string
		lookup   credentialLookupFunc
		want     *buckets.Credential
		wantErr  error
		wantText string
	}{
		{
			name:   "success",
			bucket: "bucket",
			lookup: func(context.Context, string) (*buckets.Credential, error) { return cred, nil },
			want:   cred,
		},
		{
			name:    "lookup error",
			bucket:  "bucket",
			lookup:  func(context.Context, string) (*buckets.Credential, error) { return nil, wantErr },
			wantErr: wantErr,
		},
		{
			name:     "nil credential",
			bucket:   "bucket",
			lookup:   func(context.Context, string) (*buckets.Credential, error) { return nil, nil },
			wantText: "credential not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager(tt.lookup, config.SigningConfig{})
			got, err := manager.credentialForBucket(context.Background(), tt.bucket)
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("credentialForBucket error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantText != "" && (err == nil || !strings.Contains(err.Error(), tt.wantText)) {
				t.Fatalf("credentialForBucket error = %v, want text %q", err, tt.wantText)
			}
			if tt.wantErr == nil && tt.wantText == "" && err != nil {
				t.Fatalf("credentialForBucket unexpected error = %v", err)
			}
			if tt.wantErr == nil && tt.want != nil && got != tt.want {
				t.Fatalf("credentialForBucket returned %p, want %p", got, tt.want)
			}
			if tt.wantErr == nil && tt.want == nil && (got != nil || err == nil) {
				t.Fatalf("credentialForBucket = (%v, %v), want nil credential error", got, err)
			}
		})
	}
}
