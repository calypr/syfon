package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/buckets"
)

type credentialCaptureBackend struct {
	bindings []ProviderBinding
}

func (b *credentialCaptureBackend) Sign(_ context.Context, binding ProviderBinding, _ SignRequest) (SignedAccess, error) {
	b.bindings = append(b.bindings, binding)
	return SignedAccess{Location: "signed"}, nil
}

func (*credentialCaptureBackend) BeginMultipart(context.Context, ProviderBinding, BeginMultipartRequest) (UploadID, error) {
	return "upload", nil
}

func (*credentialCaptureBackend) SignMultipartPart(context.Context, ProviderBinding, MultipartPartRequest) (SignedAccess, error) {
	return SignedAccess{}, nil
}

func (*credentialCaptureBackend) CompleteMultipart(context.Context, ProviderBinding, CompleteMultipartRequest) error {
	return nil
}

type credentialLookupFunc func(context.Context, string) (*buckets.Credential, error)

func (f credentialLookupFunc) GetS3Credential(ctx context.Context, candidate string) (*buckets.Credential, error) {
	return f(ctx, candidate)
}

func newCacheTestManager(t *testing.T, lookup CredentialLookup, backend *credentialCaptureBackend) *Manager {
	t.Helper()
	manager, err := NewManager(lookup, NewRegistration("s3", backend))
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	return manager
}

func cacheTestTarget(candidate, key string) Target {
	return Target{LookupCandidates: []string{candidate}, Key: key}
}

func TestCredentialCacheUsesExactCandidateAndIsolatedManagerEntries(t *testing.T) {
	lookup := &fakeLookup{credentials: map[string]*buckets.Credential{
		"Credential-A": {Provider: "s3", Bucket: "bucket-a", AccessKey: "first"},
		"credential-a": {Provider: "s3", Bucket: "bucket-a", AccessKey: "second"},
	}}
	backend := &credentialCaptureBackend{}
	manager := newCacheTestManager(t, lookup, backend)
	ctx := WithCredentialCache(context.Background())

	for _, candidate := range []string{"Credential-A", "Credential-A", "credential-a", "credential-a"} {
		if _, err := manager.Sign(ctx, SignRequest{Target: cacheTestTarget(candidate, "object")}); err != nil {
			t.Fatalf("Sign(%q) returned error: %v", candidate, err)
		}
	}
	if got, want := lookup.queries, []string{"Credential-A", "credential-a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("credential lookups = %#v, want exact candidate reads %#v", got, want)
	}
	if got, want := []string{backend.bindings[0].Credential.AccessKey, backend.bindings[1].Credential.AccessKey, backend.bindings[2].Credential.AccessKey, backend.bindings[3].Credential.AccessKey}, []string{"first", "first", "second", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bound credentials = %#v, want %#v", got, want)
	}

	otherLookup := &fakeLookup{credentials: map[string]*buckets.Credential{
		"Credential-A": {Provider: "s3", Bucket: "other-bucket", AccessKey: "other-manager"},
	}}
	otherBackend := &credentialCaptureBackend{}
	otherManager := newCacheTestManager(t, otherLookup, otherBackend)
	if _, err := otherManager.Sign(ctx, SignRequest{Target: cacheTestTarget("Credential-A", "object")}); err != nil {
		t.Fatalf("Sign through second manager returned error: %v", err)
	}
	if got := otherLookup.queries; !reflect.DeepEqual(got, []string{"Credential-A"}) {
		t.Fatalf("second manager lookups = %#v, want its own lookup", got)
	}
	if got := otherBackend.bindings[0].Credential.AccessKey; got != "other-manager" {
		t.Fatalf("second manager reused another manager's credential %q", got)
	}
}

func TestCredentialCacheStoresMissingAndFailedLookups(t *testing.T) {
	lookupErr := errors.New("credential database unavailable")
	lookup := &fakeLookup{errors: map[string]error{"failed": lookupErr}}
	manager := newCacheTestManager(t, lookup, &credentialCaptureBackend{})
	ctx := WithCredentialCache(context.Background())

	for _, candidate := range []string{"missing", "failed"} {
		for range 2 {
			_, err := manager.BeginMultipart(ctx, BeginMultipartRequest{Target: cacheTestTarget(candidate, "object")})
			if err == nil {
				t.Fatalf("BeginMultipart(%q) unexpectedly succeeded", candidate)
			}
			if candidate == "failed" && !errors.Is(err, lookupErr) {
				t.Fatalf("failed lookup error = %v, want wrapped %v", err, lookupErr)
			}
		}
	}
	if got, want := lookup.queries, []string{"missing", "failed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("credential lookups = %#v, want nil and failed results cached %#v", got, want)
	}
}

func TestCredentialCacheRespectsCancellationAndDoesNotCacheContextErrors(t *testing.T) {
	credential := &buckets.Credential{Provider: "s3", Bucket: "bucket", AccessKey: "current"}
	lookup := &fakeLookup{credentials: map[string]*buckets.Credential{"bucket": credential}}
	manager := newCacheTestManager(t, lookup, &credentialCaptureBackend{})
	ctx := WithCredentialCache(context.Background())
	target := Target{PhysicalBucket: "bucket", Key: "object"}
	if _, err := manager.Sign(ctx, SignRequest{Target: target}); err != nil {
		t.Fatalf("initial Sign returned error: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := manager.Sign(canceled, SignRequest{Target: target}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sign with canceled context error = %v, want context.Canceled", err)
	}
	if got := lookup.queries; !reflect.DeepEqual(got, []string{"bucket"}) {
		t.Fatalf("lookup after canceled cache hit = %#v, want cached success only", got)
	}
	if got := len(manager.providers["s3"].complete.(*credentialCaptureBackend).bindings); got != 1 {
		t.Fatalf("backend calls after canceled cache hit = %d, want only the initial call", got)
	}

	calls := 0
	cancellationLookup := credentialLookupFunc(func(_ context.Context, _ string) (*buckets.Credential, error) {
		calls++
		if calls == 1 {
			return nil, context.Canceled
		}
		return credential, nil
	})
	cancellationManager := newCacheTestManager(t, cancellationLookup, &credentialCaptureBackend{})
	cancellationCtx := WithCredentialCache(context.Background())
	for i := 0; i < 2; i++ {
		_, err := cancellationManager.BeginMultipart(cancellationCtx, BeginMultipartRequest{Target: cacheTestTarget("bucket", "object")})
		if i == 0 && !errors.Is(err, context.Canceled) {
			t.Fatalf("first lookup error = %v, want context.Canceled", err)
		}
		if i == 1 && err != nil {
			t.Fatalf("second lookup after context error returned %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("credential lookups after context error = %d, want retry", calls)
	}
}

type delayedCredentialLookup struct {
	credential *buckets.Credential
	delay      time.Duration
	calls      atomic.Int64
}

func (l *delayedCredentialLookup) GetS3Credential(_ context.Context, _ string) (*buckets.Credential, error) {
	l.calls.Add(1)
	time.Sleep(l.delay)
	return l.credential, nil
}

func BenchmarkCredentialLookupCache(b *testing.B) {
	for _, tc := range []struct {
		name   string
		cached bool
	}{
		{name: "uncached"},
		{name: "batch_cache", cached: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			lookup := &delayedCredentialLookup{
				credential: &buckets.Credential{Provider: "s3", Bucket: "bucket"},
				delay:      time.Millisecond,
			}
			backend := &fakeBackend{provider: "s3"}
			manager, err := NewManager(lookup, NewRegistration("s3", backend))
			if err != nil {
				b.Fatal(err)
			}
			targets := make([]Target, 7)
			for i := range targets {
				targets[i] = Target{PhysicalBucket: "bucket", Key: fmt.Sprintf("object-%d", i)}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx := context.Background()
				if tc.cached {
					ctx = WithCredentialCache(ctx)
				}
				for _, target := range targets {
					if _, err := manager.Sign(ctx, SignRequest{Target: target}); err != nil {
						b.Fatal(err)
					}
				}
				backend.accessRequests = backend.accessRequests[:0]
			}
			b.ReportMetric(float64(lookup.calls.Load())/float64(b.N*len(targets)), "lookups/object")
		})
	}
}
