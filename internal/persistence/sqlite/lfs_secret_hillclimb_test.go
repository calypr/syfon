package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	transferlfs "github.com/calypr/syfon/internal/transfers/lfs"
)

type hillclimbLFSObjects struct{}

func (hillclimbLFSObjects) GetObject(context.Context, string, string) (*drs.DrsObject, error) {
	return nil, errorapi.ErrObjectNotFound
}

func (hillclimbLFSObjects) RegisterObjectsIfPending(context.Context, []drs.DrsObject, objects.PendingRegistration) ([]drs.DrsObject, error) {
	return nil, nil
}

type hillclimbLFSVisibility struct {
	buckets.VisibilityQuery
	metadataCalls int
}

func (v *hillclimbLFSVisibility) ListCredentialMetadata(ctx context.Context) ([]buckets.CredentialMetadata, error) {
	v.metadataCalls++
	return v.VisibilityQuery.ListCredentialMetadata(ctx)
}

func TestHillclimbLFSSecretDecodeProfile(t *testing.T) {
	for _, credentialCount := range []int{1, 100} {
		for _, fileCount := range []int{1, 100} {
			codec := &visibilityCountingCodec{}
			db, err := NewSqliteDB(t.TempDir()+"/lfs-profile.db", codec)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < credentialCount; i++ {
				credential := &buckets.Credential{
					CredentialID: fmt.Sprintf("credential-%03d", i), Bucket: fmt.Sprintf("bucket-%03d", i),
					Provider: "s3", AccessKey: "synthetic-access", SecretKey: "synthetic-secret",
				}
				if err := db.SaveS3Credential(context.Background(), credential); err != nil {
					t.Fatal(err)
				}
			}
			visibility := &hillclimbLFSVisibility{VisibilityQuery: db}
			bucketService, err := buckets.NewService(buckets.Dependencies{Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: visibility}, nil)
			if err != nil {
				t.Fatal(err)
			}
			metadata, err := db.ListCredentialMetadata(context.Background())
			if err != nil || len(metadata) != credentialCount {
				t.Fatalf("credential metadata=%d error=%v", len(metadata), err)
			}
			service := transferlfs.NewService(nil, hillclimbLFSObjects{}, bucketService, nil, nil, nil)
			session := access.NewSession("local")
			session.AuthzEnforced = true
			session.SetAuthorizations(nil, map[string]map[string]bool{"/data_file": {"create": true}}, true)
			ctx := access.WithSession(context.Background(), session)
			request := transferlfs.BatchRequest{Operation: "upload", Objects: make([]transferlfs.BatchObject, fileCount)}
			for i := range request.Objects {
				request.Objects[i] = transferlfs.BatchObject{OID: fmt.Sprintf("%064x", i+1), Size: int64(i + 1)}
			}
			for sample := 1; sample <= 3; sample++ {
				codec.parseCalls = 0
				visibility.metadataCalls = 0
				result, err := service.Batch(ctx, request)
				if err != nil || len(result.Objects) != fileCount {
					t.Fatalf("files=%d credentials=%d sample=%d objects=%d error=%v", fileCount, credentialCount, sample, len(result.Objects), err)
				}
				for i, item := range result.Objects {
					if item.Err != nil || item.Existing || item.Size != request.Objects[i].Size || item.OID != request.Objects[i].OID {
						t.Fatalf("files=%d credentials=%d sample=%d item[%d]=%+v", fileCount, credentialCount, sample, i, item)
					}
				}
				if codec.parseCalls < 0 || codec.parseCalls > fileCount*credentialCount || visibility.metadataCalls < 0 || visibility.metadataCalls > fileCount {
					t.Fatalf("files=%d credentials=%d sample=%d parses=%d metadata=%d", fileCount, credentialCount, sample, codec.parseCalls, visibility.metadataCalls)
				}
				t.Logf("lfs_profile files=%d credentials=%d sample=%d secret_parses=%d metadata_lists=%d", fileCount, credentialCount, sample, codec.parseCalls, visibility.metadataCalls)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
