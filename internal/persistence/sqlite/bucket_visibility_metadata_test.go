package sqlite

import (
	"context"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
)

func TestBucketVisibilityReadsMetadataWhenSecretDecodeFails(t *testing.T) {
	ctx := context.Background()
	codec := &visibilityCountingCodec{}
	db, err := NewSqliteDB(t.TempDir()+"/metadata.db", codec)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	credential := &buckets.Credential{CredentialID: "credential-id", Bucket: "physical-bucket", Provider: "s3", Region: "us-east-1", AccessKey: "secret-access", SecretKey: "secret-key", Endpoint: "http://127.0.0.1:1"}
	if err := db.SaveS3Credential(ctx, credential); err != nil {
		t.Fatal(err)
	}
	service, err := buckets.NewService(buckets.Dependencies{Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: db}, nil)
	if err != nil {
		t.Fatal(err)
	}
	codec.failParse = true
	visible, err := service.ListVisibleBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if codec.parseCalls != 0 {
		t.Fatalf("metadata visibility parsed secrets %d times", codec.parseCalls)
	}
	entry, ok := visible[credential.CredentialID]
	if !ok || entry.Credential != (buckets.CredentialMetadata{CredentialID: credential.CredentialID, Bucket: credential.Bucket, Provider: credential.Provider, Region: credential.Region, Endpoint: credential.Endpoint}) {
		t.Fatalf("visible metadata = %+v", visible)
	}
	if _, err := db.GetS3Credential(ctx, credential.CredentialID); err == nil {
		t.Fatal("full credential read did not decode secrets")
	}
	if codec.parseCalls != 1 {
		t.Fatalf("full read parse calls=%d, want 1", codec.parseCalls)
	}
}

func TestBucketVisibilityPropagatesMetadataReadError(t *testing.T) {
	db, err := NewSqliteDB(t.TempDir()+"/closed.db", &visibilityCountingCodec{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := buckets.NewService(buckets.Dependencies{Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: db}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListVisibleBuckets(context.Background()); err == nil {
		t.Fatalf("metadata read error = %v", err)
	}
}
