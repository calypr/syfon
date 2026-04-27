package bucket

import (
	"context"
	"testing"

	bucketapi "github.com/calypr/syfon/apigen/client/bucketapi"
)

func TestParseStorageLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rawURL     string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{name: "valid s3 url", rawURL: "s3://bucket/path/to/file.txt", wantBucket: "bucket", wantKey: "path/to/file.txt"},
		{name: "trim key slashes", rawURL: "s3://bucket//nested/file.txt/", wantBucket: "bucket", wantKey: "nested/file.txt"},
		{name: "empty", rawURL: "", wantErr: true},
		{name: "parse error", rawURL: "://bad", wantErr: true},
		{name: "missing bucket", rawURL: "s3:///path/to/file.txt", wantErr: true},
		{name: "missing key", rawURL: "s3://bucket/", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucketName, key, err := ParseStorageLocation(tc.rawURL)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.rawURL)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseStorageLocation returned error: %v", err)
			}
			if bucketName != tc.wantBucket || key != tc.wantKey {
				t.Fatalf("unexpected parse result: bucket=%q key=%q want bucket=%q key=%q", bucketName, key, tc.wantBucket, tc.wantKey)
			}
		})
	}
}

func TestValidateBucketNonS3Providers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, provider := range []string{"gcs", "gs", "azure", "custom"} {
		provider := provider
		t.Run(provider, func(t *testing.T) {
			req := bucketapi.PutBucketRequest{
				Bucket:       "bucket-1",
				Organization: "org",
				ProjectId:    "proj",
				Provider:     &provider,
			}
			if provider != "custom" {
				logBucket := "logs"
				logPrefix := "provider-logs"
				req.BillingLogBucket = &logBucket
				req.BillingLogPrefix = &logPrefix
			}
			if err := ValidateBucket(ctx, req); err != nil {
				t.Fatalf("ValidateBucket returned error for provider %q: %v", provider, err)
			}
		})
	}
}
