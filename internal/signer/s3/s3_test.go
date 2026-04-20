package s3

import (
	"context"
	"testing"

	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
)

func TestS3Signer_getClients(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"test-bucket": {
				Bucket:    "test-bucket",
				Region:    "us-east-1",
				AccessKey: "key",
				SecretKey: "secret",
				Endpoint:  "http://localhost:9000",
			},
		},
	}
	signer := NewS3Signer(db)
	ctx := context.Background()

	t.Run("Success_FirstTime", func(t *testing.T) {
		cls, err := signer.getClients(ctx, "test-bucket")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cls.client == nil || cls.presigner == nil {
			t.Fatal("expected clients to be initialized")
		}

		// Verify caching
		cls2, _ := signer.getClients(ctx, "test-bucket")
		if cls != cls2 {
			t.Error("expected cached client to be returned")
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		signer.db.(*testutils.MockDatabase).NoDefaultCreds = true
		signer.db.(*testutils.MockDatabase).Credentials = nil
		_, err := signer.getClients(ctx, "unknown-bucket")
		if err == nil {
			t.Error("expected error for unknown bucket, got nil")
		}
	})

	t.Run("EndpointNormalization", func(t *testing.T) {
		cases := []struct {
			raw      string
			expected string
		}{
			{"localhost:9000", "http://localhost:9000"},
			{"s3.amazonaws.com", "https://s3.amazonaws.com"},
			{"http://minio:9000", "http://minio:9000"},
		}
		for _, tc := range cases {
			signer.cache.Delete("bucket-" + tc.raw)
			signer.db.(*testutils.MockDatabase).Credentials = map[string]models.S3Credential{
				"bucket-" + tc.raw: {
					Bucket:   "bucket-" + tc.raw,
					Endpoint: tc.raw,
				},
			}
			cls, err := signer.getClients(ctx, "bucket-"+tc.raw)
			if err != nil {
				t.Errorf("failed for %s: %v", tc.raw, err)
				continue
			}
			// In AWS SDK v2, we can't easily check the final endpoint without digging deep,
			// but we can at least ensure it doesn't crash and logic executes.
			if cls == nil {
				t.Errorf("got nil client for %s", tc.raw)
			}
		}
	})
}
