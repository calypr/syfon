package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/buckets"
	providerstorage "github.com/calypr/syfon/internal/storage"
)

type hillclimbProbe struct{ calls atomic.Int64 }

func (p *hillclimbProbe) Probe(_ context.Context, targets []providerstorage.ProbeTarget) []providerstorage.ProbeResult {
	p.calls.Add(1)
	target := targets[0].Target
	return []providerstorage.ProbeResult{{ID: targets[0].ID, Target: target, Metadata: providerstorage.ObjectMetadata{
		Provider: "s3", Bucket: target.PhysicalBucket, Key: target.Key, SizeBytes: 50, MetaSHA256: "abc",
	}}}
}

func TestHillclimbProbeProfile(t *testing.T) {
	for _, count := range []int{1, 100} {
		for sample := 1; sample <= 3; sample++ {
			probe := &hillclimbProbe{}
			service := NewService(Dependencies{
				ScopeResolver: fakeScopeResolver{scope: buckets.StorageScope{
					Provider: "s3", Bucket: "bucket", Prefix: "prefix/project", Prefixes: []string{"prefix", "project"},
					Credential: buckets.Credential{CredentialID: "cred", Bucket: "bucket", Provider: "s3"},
				}},
				Providers: Providers{Probe: probe},
			})
			requests := make([]internalapi.InternalInspectObjectRequest, count)
			for i := range requests {
				size := int64(i)
				requests[i] = internalapi.InternalInspectObjectRequest{Id: fmt.Sprintf("input-%d", i), Organization: "org", Project: "project", Key: "file.bin", ExpectedSizeBytes: &size}
			}
			results := service.ProbeObjects(context.Background(), requests)
			for i, result := range results {
				matched := i == 50
				if result.Id != requests[i].Id || result.Status != "present" || result.SizeMatch == nil || *result.SizeMatch != matched ||
					(matched && result.ValidationStatus != "matched") || (!matched && result.ValidationStatus != "mismatched") {
					t.Fatalf("count=%d sample=%d item=%d result=%+v", count, sample, i, result)
				}
			}
			if got := probe.calls.Load(); got < 1 || got > int64(count) {
				t.Fatalf("count=%d sample=%d probe calls=%d", count, sample, got)
			}
			t.Logf("probe_profile items=%d sample=%d physical_calls=%d", count, sample, probe.calls.Load())
		}
	}
}
