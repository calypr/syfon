package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	providerstorage "github.com/calypr/syfon/internal/storage"
)

type deleteProfileCredentials struct {
	lookups int
	value   buckets.Credential
}

func (f *deleteProfileCredentials) GetS3Credential(context.Context, string) (*buckets.Credential, error) {
	f.lookups++
	value := f.value
	return &value, nil
}

func (f *deleteProfileCredentials) ListS3Credentials(context.Context) ([]buckets.Credential, error) {
	return []buckets.Credential{f.value}, nil
}

type deleteProfileProvider struct {
	providerstorage.Provider
	deleted []providerstorage.PhysicalTarget
}

func (f *deleteProfileProvider) Delete(_ context.Context, _ providerstorage.ProviderBinding, targets []providerstorage.PhysicalTarget) error {
	f.deleted = append(f.deleted, targets...)
	return nil
}

func TestDeleteCredentialProfile(t *testing.T) {
	for _, size := range []int{1, 100} {
		for _, operation := range []string{"direct", "project"} {
			t.Run(fmt.Sprintf("%s_%d", operation, size), func(t *testing.T) {
				var durations []time.Duration
				for sample := 0; sample < 3; sample++ {
					credential := buckets.Credential{CredentialID: "cred", Provider: "s3", Bucket: "bucket"}
					reader := &deleteProfileCredentials{value: credential}
					provider := &deleteProfileProvider{}
					manager, err := providerstorage.NewManager(reader, providerstorage.NewRegistration("s3", provider))
					if err != nil {
						t.Fatal(err)
					}
					urls := make([]string, size)
					targets := make([]providerstorage.DeleteTarget, size)
					for i := range urls {
						urls[i] = fmt.Sprintf("s3://bucket/prefix/project/key-%03d", i)
						targets[i] = providerstorage.DeleteTarget{Location: urls[i]}
					}
					start := time.Now()
					switch operation {
					case "direct":
						err = manager.DeleteExact(context.Background(), targets)
					case "project":
						scope := buckets.StorageScope{Provider: "s3", Bucket: "bucket", Prefix: "prefix/project", Credential: credential}
						service := NewService(Dependencies{
							ScopeResolver: fakeScopeResolver{scope: scope}, Credentials: reader,
							Visibility: &fakeVisibility{values: map[string]buckets.VisibleBucket{"cred": {Credential: credentialMetadata(credential)}}},
							Providers:  Providers{Delete: manager},
						})
						results := service.DeleteProjectObjects(context.Background(), "org", "project", urls)
						for i, result := range results {
							if result.Status != "deleted" || result.ObjectUrl != urls[i] {
								t.Fatalf("result[%d] = %+v", i, result)
							}
						}
					}
					durations = append(durations, time.Since(start))
					if err != nil {
						t.Fatal(err)
					}
					if len(provider.deleted) != size {
						t.Fatalf("deleted %d targets, want %d", len(provider.deleted), size)
					}
					t.Logf("DELETE_CREDENTIAL_PROFILE operation=%s size=%d sample=%d lookups=%d", operation, size, sample, reader.lookups)
				}
				sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
				t.Logf("DELETE_CREDENTIAL_PROFILE operation=%s size=%d median=%s samples=%v", operation, size, durations[1], durations)
			})
		}
	}
}
