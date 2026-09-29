package sqlite

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	clientaccess "github.com/calypr/syfon/client/access"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
)

type visibilityCountingCodec struct {
	parseCalls int
	failParse  bool
}

func (c *visibilityCountingCodec) Prepare(_ context.Context, credential *buckets.Credential) (*buckets.Credential, error) {
	copy := *credential
	return &copy, nil
}
func (c *visibilityCountingCodec) Parse(_ context.Context, credential *buckets.Credential) (*buckets.Credential, error) {
	c.parseCalls++
	if c.failParse {
		return nil, fmt.Errorf("synthetic secret decode failure")
	}
	copy := *credential
	return &copy, nil
}
func (*visibilityCountingCodec) Enabled() (bool, error) { return false, nil }

func TestBucketVisibilityProfile(t *testing.T) {
	for _, size := range []int{1, 100} {
		for _, restricted := range []bool{false, true} {
			t.Run(fmt.Sprintf("size_%d_restricted_%t", size, restricted), func(t *testing.T) {
				ctx := context.Background()
				codec := &visibilityCountingCodec{}
				db, err := NewSqliteDB(t.TempDir()+"/visibility.db", codec)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				privileges := make(map[string]map[string]bool)
				wantVisible := size
				if restricted {
					wantVisible = (size + 1) / 2
				}
				for i := 0; i < size; i++ {
					id := fmt.Sprintf("credential-%03d", i)
					bucket := fmt.Sprintf("bucket-%03d", i)
					project := fmt.Sprintf("project-%03d", i)
					if err := db.SaveS3Credential(ctx, &buckets.Credential{CredentialID: id, Bucket: bucket, Provider: "s3", Region: "us-east-1", AccessKey: "synthetic-access", SecretKey: "synthetic-secret", Endpoint: "http://127.0.0.1:1"}); err != nil {
						t.Fatal(err)
					}
					if err := db.CreateBucketScope(ctx, &buckets.Scope{Organization: "profile-org", ProjectID: project, CredentialID: id, Bucket: bucket}); err != nil {
						t.Fatal(err)
					}
					if i%2 == 0 {
						resource, err := clientaccess.ResourcePath("profile-org", project)
						if err != nil {
							t.Fatal(err)
						}
						privileges[resource] = map[string]bool{"read": true}
					}
				}
				service, err := buckets.NewService(buckets.Dependencies{Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: db}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if restricted {
					session := access.NewSession("gen3")
					session.AuthHeaderPresent = true
					session.SetAuthorizations(nil, privileges, true)
					ctx = access.WithSession(ctx, session)
				}
				var samples []time.Duration
				var parses int
				for run := 0; run < 3; run++ {
					codec.parseCalls = 0
					start := time.Now()
					visible, err := service.ListVisibleBuckets(ctx)
					samples = append(samples, time.Since(start))
					if err != nil {
						t.Fatal(err)
					}
					if len(visible) != wantVisible {
						t.Fatalf("visible=%d, want %d", len(visible), wantVisible)
					}
					parses = codec.parseCalls
				}
				sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
				fmt.Fprintf(os.Stdout, "BUCKET_PROFILE size=%d restricted=%t median=%s samples=%v parse=%d visible=%d\n", size, restricted, samples[1], samples, parses, wantVisible)
				codec.parseCalls = 0
				if _, err := db.GetS3Credential(ctx, "credential-000"); err != nil {
					t.Fatal(err)
				}
				if codec.parseCalls != 1 {
					t.Fatalf("full credential read parse calls=%d, want 1", codec.parseCalls)
				}
			})
		}
	}
}
