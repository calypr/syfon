package transfers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/storage"
)

type multipartBatchCountingStore struct {
	MultipartSessionStore
	reads   int
	touches int
}

func (s *multipartBatchCountingStore) GetMultipartSession(ctx context.Context, uploadID string) (MultipartSession, error) {
	s.reads++
	return s.MultipartSessionStore.GetMultipartSession(ctx, uploadID)
}

func (s *multipartBatchCountingStore) TouchMultipartSession(ctx context.Context, uploadID string, now time.Time) error {
	s.touches++
	return s.MultipartSessionStore.TouchMultipartSession(ctx, uploadID, now)
}

type multipartBatchTestStorage struct {
	calls  []int32
	onSign func(context.Context, storage.MultipartPartRequest) (storage.SignedAccess, error)
}

func (*multipartBatchTestStorage) Sign(context.Context, storage.SignRequest) (storage.SignedAccess, error) {
	return storage.SignedAccess{}, nil
}

func (*multipartBatchTestStorage) BeginMultipart(context.Context, storage.BeginMultipartRequest) (storage.UploadID, error) {
	return "", nil
}

func (s *multipartBatchTestStorage) SignMultipartPart(ctx context.Context, request storage.MultipartPartRequest) (storage.SignedAccess, error) {
	s.calls = append(s.calls, request.PartNumber)
	if s.onSign != nil {
		return s.onSign(ctx, request)
	}
	return storage.SignedAccess{Location: fmt.Sprintf("part-%d", request.PartNumber)}, nil
}

func (*multipartBatchTestStorage) CompleteMultipart(context.Context, storage.CompleteMultipartRequest) error {
	return nil
}

type multipartBatchCredentialLookup struct {
	reads int
}

func (l *multipartBatchCredentialLookup) GetS3Credential(_ context.Context, bucket string) (*buckets.Credential, error) {
	l.reads++
	return &buckets.Credential{Bucket: bucket, Provider: "s3", AccessKey: fmt.Sprintf("key-%d", l.reads)}, nil
}

type multipartBatchCredentialProvider struct {
	seen []string
}

func (*multipartBatchCredentialProvider) Sign(context.Context, storage.ProviderBinding, storage.SignRequest) (storage.SignedAccess, error) {
	return storage.SignedAccess{}, nil
}

func (*multipartBatchCredentialProvider) BeginMultipart(context.Context, storage.ProviderBinding, storage.BeginMultipartRequest) (storage.UploadID, error) {
	return "", nil
}

func (p *multipartBatchCredentialProvider) SignMultipartPart(_ context.Context, binding storage.ProviderBinding, request storage.MultipartPartRequest) (storage.SignedAccess, error) {
	p.seen = append(p.seen, binding.Credential.AccessKey)
	return storage.SignedAccess{Location: fmt.Sprintf("key-%s-part-%d", binding.Credential.AccessKey, request.PartNumber)}, nil
}

func (*multipartBatchCredentialProvider) CompleteMultipart(context.Context, storage.ProviderBinding, storage.CompleteMultipartRequest) error {
	return nil
}

func newMultipartBatchTestService(t *testing.T, provider StoragePort, authorization MultipartAuthorization) (*Service, *multipartBatchCountingStore, *memoryMultipartSessionStore, time.Time) {
	t.Helper()
	created := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	base := newMemoryMultipartSessionStore()
	if err := base.SaveMultipartSession(context.Background(), MultipartSession{
		UploadID: "upload", CompletionID: "completion",
		Target:        storage.Target{Provider: "s3", PhysicalBucket: "bucket", LookupKey: "bucket", Key: "object"},
		Authorization: authorization, State: MultipartStateActive,
		CreatedAt: created, UpdatedAt: created,
	}); err != nil {
		t.Fatal(err)
	}
	counted := &multipartBatchCountingStore{MultipartSessionStore: base}
	service := NewService(Dependencies{
		Storage: provider, MultipartSessions: counted,
		Now: func() time.Time { return created.Add(time.Minute) },
	})
	return service, counted, base, created
}

func TestSignMultipartPartsUsesFreshCredentialCachePerRequest(t *testing.T) {
	lookup := &multipartBatchCredentialLookup{}
	provider := &multipartBatchCredentialProvider{}
	manager, err := storage.NewManager(lookup, storage.NewRegistration("s3", provider))
	if err != nil {
		t.Fatal(err)
	}
	service, store, _, _ := newMultipartBatchTestService(t, manager, MultipartAuthorization{})
	for _, batch := range [][]int32{{1, 2}, {3}} {
		if _, err := service.SignMultipartParts(context.Background(), "upload", batch); err != nil {
			t.Fatalf("SignMultipartParts(%v): %v", batch, err)
		}
	}
	if lookup.reads != 2 || store.reads != 2 || store.touches != 2 {
		t.Fatalf("batch setup counts credentials=%d session_reads=%d touches=%d, want 2 each", lookup.reads, store.reads, store.touches)
	}
	want := []string{"key-1", "key-1", "key-2"}
	if fmt.Sprint(provider.seen) != fmt.Sprint(want) {
		t.Fatalf("provider credentials = %v, want %v", provider.seen, want)
	}
}

func TestSignMultipartPartsRejectsUnauthorizedSessionBeforeProviderCalls(t *testing.T) {
	provider := &multipartBatchTestStorage{}
	authorization := MultipartAuthorization{Resources: []string{"/object"}, Methods: []string{"update"}}
	service, store, _, _ := newMultipartBatchTestService(t, provider, authorization)
	session := access.NewSession("gen3")
	session.SetAuthorizations(nil, nil, true)
	ctx := access.WithSession(context.Background(), session)

	if _, err := service.SignMultipartParts(ctx, "upload", []int32{1, 2}); !errors.Is(err, errorapi.ErrAccessDenied) {
		t.Fatalf("SignMultipartParts() error = %v, want access denied", err)
	}
	if len(provider.calls) != 0 || store.touches != 0 {
		t.Fatalf("unauthorized batch reached provider/touch: provider calls=%v touches=%d", provider.calls, store.touches)
	}
}

func TestSignMultipartPartsFailureAndCancellationDoNotTouchSession(t *testing.T) {
	providerFailure := errors.New("provider signing failed")
	for _, testCase := range []struct {
		name    string
		prepare func() (context.Context, func(), *multipartBatchTestStorage)
		wantErr error
		wantN   int
	}{
		{
			name: "provider failure",
			prepare: func() (context.Context, func(), *multipartBatchTestStorage) {
				provider := &multipartBatchTestStorage{onSign: func(_ context.Context, request storage.MultipartPartRequest) (storage.SignedAccess, error) {
					if request.PartNumber == 2 {
						return storage.SignedAccess{}, providerFailure
					}
					return storage.SignedAccess{Location: "part"}, nil
				}}
				return context.Background(), func() {}, provider
			},
			wantErr: providerFailure,
			wantN:   2,
		},
		{
			name: "cancellation",
			prepare: func() (context.Context, func(), *multipartBatchTestStorage) {
				ctx, cancel := context.WithCancel(context.Background())
				provider := &multipartBatchTestStorage{onSign: func(_ context.Context, _ storage.MultipartPartRequest) (storage.SignedAccess, error) {
					cancel()
					return storage.SignedAccess{Location: "part"}, nil
				}}
				return ctx, cancel, provider
			},
			wantErr: context.Canceled,
			wantN:   1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cleanup, provider := testCase.prepare()
			defer cleanup()
			service, store, base, created := newMultipartBatchTestService(t, provider, MultipartAuthorization{})
			if _, err := service.SignMultipartParts(ctx, "upload", []int32{1, 2}); !errors.Is(err, testCase.wantErr) {
				t.Fatalf("SignMultipartParts() error = %v, want %v", err, testCase.wantErr)
			}
			if len(provider.calls) != testCase.wantN || store.touches != 0 {
				t.Fatalf("provider calls=%v touches=%d, want %d calls and no touch", provider.calls, store.touches, testCase.wantN)
			}
			session, err := base.GetMultipartSession(context.Background(), "upload")
			if err != nil {
				t.Fatal(err)
			}
			if !session.UpdatedAt.Equal(created) {
				t.Fatalf("failed batch updated activity to %s, want %s", session.UpdatedAt, created)
			}
		})
	}
}

func TestSignMultipartPartsRejectsInvalidBatchesBeforeSessionRead(t *testing.T) {
	tests := []struct {
		name        string
		partNumbers []int32
	}{
		{name: "empty"},
		{name: "too many", partNumbers: make([]int32, maxMultipartPartURLBatch+1)},
		{name: "duplicate", partNumbers: []int32{1, 1}},
		{name: "nonpositive", partNumbers: []int32{0}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			provider := &multipartBatchTestStorage{}
			service, store, _, _ := newMultipartBatchTestService(t, provider, MultipartAuthorization{})
			if _, err := service.SignMultipartParts(context.Background(), "upload", testCase.partNumbers); !errors.Is(err, errorapi.ErrInvalidInput) {
				t.Fatalf("SignMultipartParts() error = %v, want invalid input", err)
			}
			if store.reads != 0 || store.touches != 0 || len(provider.calls) != 0 {
				t.Fatalf("invalid batch reached session/provider: reads=%d touches=%d signs=%v", store.reads, store.touches, provider.calls)
			}
		})
	}
}
