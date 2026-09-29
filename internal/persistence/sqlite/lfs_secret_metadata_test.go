package sqlite

import (
	"context"
	"testing"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	transferlfs "github.com/calypr/syfon/internal/transfers/lfs"
)

func lfsPreparationContext() context.Context {
	session := access.NewSession("local")
	session.AuthzEnforced = true
	session.SetAuthorizations(nil, map[string]map[string]bool{"/data_file": {"create": true}}, true)
	return access.WithSession(context.Background(), session)
}

func TestLFSPreparationReadsMetadataWhenSecretDecodeFails(t *testing.T) {
	codec := &visibilityCountingCodec{}
	db, err := NewSqliteDB(t.TempDir()+"/lfs-secret-failure.db", codec)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SaveS3Credential(context.Background(), &buckets.Credential{
		CredentialID: "credential", Bucket: "bucket", Provider: "s3", AccessKey: "access", SecretKey: "secret",
	}); err != nil {
		t.Fatal(err)
	}
	bucketService, err := buckets.NewService(buckets.Dependencies{Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: db}, nil)
	if err != nil {
		t.Fatal(err)
	}
	codec.failParse = true
	service := transferlfs.NewService(nil, hillclimbLFSObjects{}, bucketService, nil, nil, nil)
	result, err := service.PrepareUpload(lfsPreparationContext(), "new-object", 5)
	if err != nil || result.Existing || result.Size != 5 || codec.parseCalls != 0 {
		t.Fatalf("metadata preparation=%+v error=%v secret parses=%d", result, err, codec.parseCalls)
	}
	if _, err := bucketService.ListS3Credentials(context.Background()); err == nil || codec.parseCalls != 1 {
		t.Fatalf("full credential read error=%v secret parses=%d", err, codec.parseCalls)
	}
}

func TestLFSPreparationPropagatesMetadataQueryError(t *testing.T) {
	db, err := NewSqliteDB(t.TempDir()+"/lfs-closed.db", &visibilityCountingCodec{})
	if err != nil {
		t.Fatal(err)
	}
	bucketService, err := buckets.NewService(buckets.Dependencies{Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: db}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	service := transferlfs.NewService(nil, hillclimbLFSObjects{}, bucketService, nil, nil, nil)
	if _, err := service.PrepareUpload(lfsPreparationContext(), "new-object", 5); err == nil {
		t.Fatal("metadata query error was not returned")
	}
}
