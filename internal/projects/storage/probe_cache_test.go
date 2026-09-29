package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/buckets"
	providerstorage "github.com/calypr/syfon/internal/storage"
)

type blockingProbe struct {
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
	err     error
}

type staticProbeVisibility struct {
	visible map[string]buckets.VisibleBucket
}

type waitSignalContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *waitSignalContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func (v staticProbeVisibility) ListVisibleBuckets(context.Context) (map[string]buckets.VisibleBucket, error) {
	return v.visible, nil
}

func (p *blockingProbe) Probe(_ context.Context, targets []providerstorage.ProbeTarget) []providerstorage.ProbeResult {
	if p.calls.Add(1) == 1 && p.started != nil {
		close(p.started)
		<-p.release
	}
	target := targets[0].Target
	return []providerstorage.ProbeResult{{ID: targets[0].ID, Target: target, Err: p.err, Metadata: providerstorage.ObjectMetadata{
		Provider: "s3", Bucket: target.PhysicalBucket, Key: target.Key, SizeBytes: 50, MetaSHA256: "abc",
	}}}
}

func scopedProbeService(probe ProbePort) *Service {
	return NewService(Dependencies{
		ScopeResolver: fakeScopeResolver{scope: buckets.StorageScope{
			Provider: "s3", Bucket: "bucket", Prefix: "prefix/project", Prefixes: []string{"prefix", "project"},
			Credential: buckets.Credential{CredentialID: "cred", Bucket: "bucket", Provider: "s3"},
		}},
		Providers: Providers{Probe: probe},
	})
}

func TestProbeObjectsCoalescesConcurrentMetadataAndValidatesEachInput(t *testing.T) {
	probe := &blockingProbe{started: make(chan struct{}), release: make(chan struct{})}
	service := scopedProbeService(probe)
	size := int64(50)
	wrongSize := int64(49)
	requests := []internalapi.InternalInspectObjectRequest{
		{Id: "size-match", Organization: "org", Project: "project", Key: "file.bin", ExpectedSizeBytes: &size},
		{Id: "size-mismatch", Organization: "org", Project: "project", Key: "file.bin", ExpectedSizeBytes: &wrongSize},
		{Id: "name-match", Organization: "org", Project: "project", Key: "file.bin", ExpectedName: "file.bin"},
		{Id: "name-mismatch", Organization: "org", Project: "project", Key: "file.bin", ExpectedName: "other.bin"},
		{Id: "sha-match", Organization: "org", Project: "project", Key: "file.bin", ExpectedSha256: "sha256:abc"},
		{Id: "sha-mismatch", Organization: "org", Project: "project", Key: "file.bin", ExpectedSha256: "def"},
	}
	done := make(chan []internalapi.InternalInspectObjectBulkItem, 1)
	go func() { done <- service.ProbeObjects(context.Background(), requests) }()
	select {
	case <-probe.started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not start")
	}
	close(probe.release)
	var results []internalapi.InternalInspectObjectBulkItem
	select {
	case results = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bulk probe did not finish")
	}
	if got := probe.calls.Load(); got != 1 {
		t.Fatalf("physical probes=%d, want 1", got)
	}
	for i, result := range results {
		want := "matched"
		if i%2 == 1 {
			want = "mismatched"
		}
		if result.Id != requests[i].Id || result.Status != "present" || result.ValidationStatus != want || result.ObjectUrl != "s3://bucket/prefix/project/file.bin" {
			t.Fatalf("result[%d]=%+v", i, result)
		}
	}
	if len(results[1].ValidationMismatches) != 1 || results[1].ValidationMismatches[0] != "size_mismatch" ||
		len(results[3].ValidationMismatches) != 1 || results[3].ValidationMismatches[0] != "name_mismatch" ||
		len(results[5].ValidationMismatches) != 1 || results[5].ValidationMismatches[0] != "sha256_mismatch" {
		t.Fatalf("mismatches leaked across inputs: %+v", results)
	}
	service.ProbeObjects(context.Background(), requests[:1])
	if got := probe.calls.Load(); got != 2 {
		t.Fatalf("second operation physical probes=%d, want 2", got)
	}
}

func TestProbeObjectsSeparatesScopeIdentity(t *testing.T) {
	probe := &blockingProbe{}
	service := scopedProbeService(probe)
	requests := []internalapi.InternalInspectObjectRequest{
		{Organization: "org", Project: "project", Key: "file.bin"},
		{Organization: "other-org", Project: "project", Key: "file.bin"},
		{Organization: "org", Project: "other-project", Key: "file.bin"},
		{Organization: "org", Project: "project", Key: "other.bin"},
	}
	results := service.ProbeObjects(context.Background(), requests)
	if got := probe.calls.Load(); got != int64(len(requests)) {
		t.Fatalf("physical probes=%d, want %d", got, len(requests))
	}
	for i, result := range results {
		if result.Status != "present" {
			t.Fatalf("result[%d]=%+v", i, result)
		}
	}
}

func TestProbeObjectsCoalescesRawURLAndKeepsErrorValidation(t *testing.T) {
	probe := &blockingProbe{err: &providerstorage.OperationError{Kind: providerstorage.ErrorUnavailable, Provider: "s3", Capability: "probe", Cause: errors.New("private provider detail")}}
	credential := buckets.Credential{CredentialID: "cred", Bucket: "bucket", Provider: "s3"}
	service := NewService(Dependencies{
		Credentials: fakeCredentials{values: map[string]buckets.Credential{"cred": credential}},
		Visibility:  staticProbeVisibility{visible: map[string]buckets.VisibleBucket{"cred": {Credential: credentialMetadata(credential)}}},
		Providers:   Providers{Probe: probe},
	})
	size := int64(50)
	requests := []internalapi.InternalInspectObjectRequest{
		{Id: "plain", ObjectUrl: "s3://bucket/file.bin"},
		{Id: "size", ObjectUrl: "s3://bucket/file.bin", ExpectedSizeBytes: &size},
	}
	results := service.ProbeObjects(context.Background(), requests)
	if got := probe.calls.Load(); got != 1 {
		t.Fatalf("physical probes=%d, want 1", got)
	}
	for i, result := range results {
		want := "not_requested"
		if i == 1 {
			want = "unverifiable"
		}
		if result.Id != requests[i].Id || result.Status != "error" || result.ErrorKind != "storage_unavailable" || result.ValidationStatus != want || result.SizeMatch != nil || strings.Contains(result.Error, "private provider detail") {
			t.Fatalf("result[%d]=%+v", i, result)
		}
	}
}

func TestProbeCacheWaiterCancellationDoesNotStopLeader(t *testing.T) {
	cache := &requestCache{probes: make(map[probeRequestKey]*probeEntry)}
	key := probeRequestKey{objectURL: "s3://bucket/file.bin"}
	started := make(chan struct{})
	release := make(chan struct{})
	leader := make(chan error, 1)
	go func() {
		_, err := cache.loadProbe(context.Background(), key, func() (*objectMetadata, error) {
			close(started)
			<-release
			return &objectMetadata{Key: "file.bin"}, nil
		})
		leader <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not start")
	}
	base, cancel := context.WithCancel(context.Background())
	ctx := &waitSignalContext{Context: base, entered: make(chan struct{})}
	waiter := make(chan error, 1)
	var waiterLoaderCalls atomic.Int64
	go func() {
		_, err := cache.loadProbe(ctx, key, func() (*objectMetadata, error) {
			waiterLoaderCalls.Add(1)
			return nil, nil
		})
		waiter <- err
	}()
	select {
	case <-ctx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not reach in-flight entry")
	}
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error=%v, want canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not return")
	}
	if got := waiterLoaderCalls.Load(); got != 0 {
		t.Fatalf("waiter loader calls=%d, want 0", got)
	}
	close(release)
	select {
	case err := <-leader:
		if err != nil {
			t.Fatalf("leader error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not finish")
	}
}

func TestProbeCacheRetriesAfterCanceledLeader(t *testing.T) {
	cache := &requestCache{probes: make(map[probeRequestKey]*probeEntry)}
	key := probeRequestKey{objectURL: "s3://bucket/file.bin"}
	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	go func() {
		_, err := cache.loadProbe(ctx, key, func() (*objectMetadata, error) {
			close(started)
			<-release
			return &objectMetadata{Key: "stale.bin"}, nil
		})
		leader <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not start")
	}
	cancel()
	close(release)
	select {
	case err := <-leader:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader error=%v, want canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled leader did not finish")
	}
	called := false
	metadata, err := cache.loadProbe(context.Background(), key, func() (*objectMetadata, error) {
		called = true
		return &objectMetadata{Key: "fresh.bin"}, nil
	})
	if err != nil || !called || metadata.Key != "fresh.bin" {
		t.Fatalf("retry metadata=%+v error=%v called=%v", metadata, err, called)
	}
}
