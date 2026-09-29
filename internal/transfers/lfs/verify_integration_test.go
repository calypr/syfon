package lfs_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/sqlite"
	"github.com/calypr/syfon/internal/persistence/store"
	lfs "github.com/calypr/syfon/internal/transfers/lfs"
)

type retryRegistration struct {
	*objects.Service
	err error
}

type restagingRegistration struct {
	*objects.Service
	pending     *store.Store
	replacement lfs.PendingMetadata
	restage     bool
}

func (r *restagingRegistration) RegisterObjectsIfPending(ctx context.Context, records []drs.DrsObject, pending objects.PendingRegistration) ([]drs.DrsObject, error) {
	if r.restage {
		r.restage = false
		if err := r.pending.SavePendingMetadata(ctx, []lfs.PendingMetadata{r.replacement}); err != nil {
			return nil, err
		}
	}
	return r.Service.RegisterObjectsIfPending(ctx, records, pending)
}

type uploadRecorder struct{}

func (uploadRecorder) RecordFileUpload(context.Context, string) error { return nil }

type concurrentUploadRecorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *concurrentUploadRecorder) RecordFileUpload(_ context.Context, objectID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, objectID)
	return nil
}

func (r *concurrentUploadRecorder) recordedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

type concurrentPendingStore struct {
	*store.Store
	arrived chan struct{}
	release chan struct{}
}

type concurrentMissingObjectLookup struct {
	*objects.Service
	mu      sync.Mutex
	lookups int
	arrived chan struct{}
	release chan struct{}
}

func (o *concurrentMissingObjectLookup) GetObject(ctx context.Context, objectID, method string) (*drs.DrsObject, error) {
	o.mu.Lock()
	o.lookups++
	lookup := o.lookups
	o.mu.Unlock()
	if lookup <= 2 {
		o.arrived <- struct{}{}
		<-o.release
		return nil, errorapi.ErrNotFound
	}
	return o.Service.GetObject(ctx, objectID, method)
}

func newConcurrentPendingStore(database *store.Store) *concurrentPendingStore {
	return &concurrentPendingStore{
		Store:   database,
		arrived: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
}

func (s *concurrentPendingStore) ConsumePendingMetadata(ctx context.Context, expected lfs.PendingMetadata) (bool, error) {
	s.arrived <- struct{}{}
	<-s.release
	return s.Store.ConsumePendingMetadata(ctx, expected)
}

func (r *retryRegistration) RegisterObjects(ctx context.Context, records []drs.DrsObject) ([]drs.DrsObject, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Service.RegisterObjects(ctx, records)
}

func (r *retryRegistration) RegisterObjectsIfPending(ctx context.Context, records []drs.DrsObject, pending objects.PendingRegistration) ([]drs.DrsObject, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.Service.RegisterObjectsIfPending(ctx, records, pending)
}

func TestVerifyRetriesRealPendingMetadataAfterRegistrationFailure(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	oid := strings.Repeat("b", 64)
	providerType, location := "s3", "s3://bucket/"+oid
	candidate := lfsapi.DrsObjectCandidate{
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{Type: &providerType, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location}}},
	}
	now := time.Now().UTC()
	pending := lfs.PendingMetadata{OID: oid, Candidate: candidate, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := database.SavePendingMetadata(ctx, []lfs.PendingMetadata{pending}); err != nil {
		t.Fatal(err)
	}
	seedDurableUploadReceipt(t, database, oid, 0, location)

	registrationError := errors.New("registration unavailable")
	registrar := &retryRegistration{Service: objects.NewService(database), err: registrationError}
	service := lfs.NewService(nil, registrar, nil, database, uploadRecorder{}, nil)
	if err := service.Verify(ctx, oid, 0); !errors.Is(err, registrationError) {
		t.Fatalf("first Verify() error = %v, want %v", err, registrationError)
	}
	if _, err := database.GetPendingMetadata(ctx, oid); err != nil {
		t.Fatalf("pending metadata after failed registration: %v", err)
	}

	registrar.err = nil
	if err := service.Verify(ctx, oid, 0); err != nil {
		t.Fatalf("retry Verify() error = %v", err)
	}
	if _, err := database.GetPendingMetadata(ctx, oid); !errorapi.IsNotFoundError(err) {
		t.Fatalf("pending metadata after successful retry error = %v, want not found", err)
	}
	if _, err := registrar.GetObject(ctx, oid, "read"); err != nil {
		t.Fatalf("registered object lookup: %v", err)
	}
	if _, err := database.GetLFSUploadReceipt(ctx, oid); err != nil {
		t.Fatalf("durable upload receipt after metadata consumption: %v", err)
	}
}

func TestVerifyDoesNotRegisterReplacedPendingMetadata(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	oid := strings.Repeat("d", 64)
	providerType, location := "s3", "s3://bucket/"+oid
	originalName, replacementName := "original", "replacement"
	candidate := lfsapi.DrsObjectCandidate{
		Name:          &originalName,
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{Type: &providerType, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location}}},
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	initial := lfs.PendingMetadata{OID: oid, Candidate: candidate, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := database.SavePendingMetadata(ctx, []lfs.PendingMetadata{initial}); err != nil {
		t.Fatal(err)
	}
	seedDurableUploadReceipt(t, database, oid, 0, location)
	replacement := initial
	replacement.Candidate.Name = &replacementName
	replacement.CreatedAt = now.Add(time.Minute)
	registrar := &restagingRegistration{Service: objects.NewService(database), pending: database, replacement: replacement, restage: true}
	service := lfs.NewService(nil, registrar, nil, database, uploadRecorder{}, nil)
	if err := service.Verify(ctx, oid, 0); !errors.Is(err, errorapi.ErrConflict) {
		t.Fatalf("Verify stale candidate error = %v, want conflict", err)
	}
	if _, err := registrar.GetObject(ctx, oid, "read"); !errorapi.IsNotFoundError(err) {
		t.Fatalf("stale metadata was registered: %v", err)
	}
	if err := service.Verify(ctx, oid, 0); err != nil {
		t.Fatalf("Verify replacement: %v", err)
	}
	object, err := registrar.GetObject(ctx, oid, "read")
	if err != nil || object.Name == nil || *object.Name != "replacement" {
		t.Fatalf("registered object = %+v, %v; want replacement", object, err)
	}
}

func TestVerifyRequiresUploadEvidenceAfterStage(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	content := []byte("staged but not uploaded")
	digest := sha256.Sum256(content)
	oid := hex.EncodeToString(digest[:])
	ctx := context.Background()
	providerType, location := "s3", "s3://bucket/"+oid
	size := int64(len(content))
	candidate := lfsapi.DrsObjectCandidate{
		Size:      &size,
		Checksums: &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{
			Type:      &providerType,
			AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location},
		}},
	}
	objectService := objects.NewService(database)
	service := lfs.NewService(nil, objectService, nil, database, uploadRecorder{}, nil)
	if err := service.Stage(ctx, []lfsapi.DrsObjectCandidate{candidate}, lfs.PendingMetadataTTL); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := service.Verify(ctx, oid, size); !errorapi.IsNotFoundError(err) {
		t.Fatalf("Verify() error = %v, want missing upload evidence", err)
	}
	if _, err := objectService.GetObject(ctx, oid, "read"); !errorapi.IsNotFoundError(err) {
		t.Fatalf("object after evidence-free Verify() error = %v, want not found", err)
	}
	if _, err := database.GetPendingMetadata(ctx, oid); err != nil {
		t.Fatalf("pending metadata after evidence-free Verify(): %v", err)
	}
}

func TestVerifyRegistersExpiredCandidateWhenReceiptExtendsPendingExpiry(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	oid := strings.Repeat("f", 64)
	providerType, location := "s3", "s3://bucket/"+oid
	size := int64(0)
	candidate := lfsapi.DrsObjectCandidate{
		Size:          &size,
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{Type: &providerType, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location}}},
	}
	originalExpiry := time.Now().UTC().Add(-time.Minute)
	pending := lfs.PendingMetadata{
		OID:       oid,
		Candidate: candidate,
		CreatedAt: originalExpiry.Add(-time.Hour),
		ExpiresAt: originalExpiry,
	}
	if err := database.SavePendingMetadata(ctx, []lfs.PendingMetadata{pending}); err != nil {
		t.Fatalf("save expired pending metadata: %v", err)
	}
	completedAt := time.Now().UTC()
	receipt := lfs.UploadReceipt{
		OID:         oid,
		Size:        size,
		SHA256:      oid,
		StorageURL:  location,
		CompletedAt: completedAt,
		ExpiresAt:   completedAt.Add(time.Hour),
	}
	if err := database.SaveLFSUploadReceipt(ctx, receipt); err != nil {
		t.Fatalf("save upload receipt: %v", err)
	}

	loaded, err := database.GetPendingMetadata(ctx, oid)
	if err != nil {
		t.Fatalf("GetPendingMetadata after receipt: %v", err)
	}
	if loaded.ExpiresAt.Before(receipt.ExpiresAt) {
		t.Fatalf("embedded candidate expiry = %s, want at least receipt expiry %s", loaded.ExpiresAt, receipt.ExpiresAt)
	}
	var databaseExpiry time.Time
	if err := database.DB().QueryRowContext(ctx, `SELECT expires_time FROM lfs_pending_metadata WHERE oid = ?`, oid).Scan(&databaseExpiry); err != nil {
		t.Fatalf("load DB expiry: %v", err)
	}
	if databaseExpiry.Before(receipt.ExpiresAt) {
		t.Fatalf("database expiry = %s, want at least receipt expiry %s", databaseExpiry, receipt.ExpiresAt)
	}

	objectService := objects.NewService(database)
	service := lfs.NewService(nil, objectService, nil, database, uploadRecorder{}, nil)
	if err := service.Verify(ctx, oid, size); err != nil {
		t.Fatalf("Verify with receipt after staged expiry: %v", err)
	}
	if _, err := objectService.GetObject(ctx, oid, "read"); err != nil {
		t.Fatalf("object was not registered after receipt extended expiry: %v", err)
	}
}

func TestVerifyKeepsExpiredCandidateWithoutReceiptUnavailable(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	oid := strings.Repeat("1", 64)
	providerType, location := "s3", "s3://bucket/"+oid
	size := int64(0)
	candidate := lfsapi.DrsObjectCandidate{
		Size:          &size,
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{Type: &providerType, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location}}},
	}
	expiredAt := time.Now().UTC().Add(-time.Minute)
	if err := database.SavePendingMetadata(ctx, []lfs.PendingMetadata{{
		OID: oid, Candidate: candidate, CreatedAt: expiredAt.Add(-time.Hour), ExpiresAt: expiredAt,
	}}); err != nil {
		t.Fatalf("save expired pending metadata: %v", err)
	}

	objectService := objects.NewService(database)
	service := lfs.NewService(nil, objectService, nil, database, uploadRecorder{}, nil)
	if err := service.Verify(ctx, oid, size); !errorapi.IsNotFoundError(err) {
		t.Fatalf("Verify without receipt = %v, want not found", err)
	}
	if _, err := objectService.GetObject(ctx, oid, "read"); !errorapi.IsNotFoundError(err) {
		t.Fatalf("expired candidate registered an object: %v", err)
	}
	if _, err := database.GetPendingMetadata(ctx, oid); !errorapi.IsNotFoundError(err) {
		t.Fatalf("expired candidate remains pending: %v", err)
	}
}

func TestVerifyCountsCompletedUploadOnlyOnce(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	oid := strings.Repeat("e", 64)
	providerType, location := "s3", "s3://bucket/"+oid
	candidate := lfsapi.DrsObjectCandidate{
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{Type: &providerType, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location}}},
	}
	now := time.Now().UTC()
	if err := database.SavePendingMetadata(ctx, []lfs.PendingMetadata{{OID: oid, Candidate: candidate, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}}); err != nil {
		t.Fatalf("save pending metadata: %v", err)
	}
	seedDurableUploadReceipt(t, database, oid, 0, location)
	if err := database.RecordFileUpload(ctx, oid); err != nil {
		t.Fatalf("record completed upload: %v", err)
	}

	service := lfs.NewService(nil, objects.NewService(database), nil, database, database, nil)
	if err := service.Verify(ctx, oid, 0); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	usage, err := database.GetFileUsage(ctx, oid)
	if err != nil {
		t.Fatalf("GetFileUsage: %v", err)
	}
	gotUploads := int64(0)
	if usage.UploadCount != nil {
		gotUploads = *usage.UploadCount
	}
	if gotUploads != 1 {
		t.Fatalf("upload count = %d, want one completed upload", gotUploads)
	}
}

func TestConcurrentVerifyReassignsUploadUsageToDurableObjectID(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	oid := strings.Repeat("c", 64)
	canonicalID := "durable-object-id"
	providerType, location := "s3", "s3://bucket/"+oid
	candidateID := canonicalID
	candidate := lfsapi.DrsObjectCandidate{
		Id:            &candidateID,
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
		AccessMethods: &[]lfsapi.AccessMethod{{Type: &providerType, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &location}}},
	}
	now := time.Now().UTC()
	pending := lfs.PendingMetadata{OID: oid, Candidate: candidate, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := database.SavePendingMetadata(ctx, []lfs.PendingMetadata{pending}); err != nil {
		t.Fatalf("save pending metadata: %v", err)
	}
	seedDurableUploadReceipt(t, database, oid, 0, location)
	if err := database.RecordFileUpload(ctx, oid); err != nil {
		t.Fatalf("record completed upload: %v", err)
	}

	pendingStore := newConcurrentPendingStore(database)
	recorder := &concurrentUploadRecorder{}
	objectLookup := &concurrentMissingObjectLookup{
		Service: objects.NewService(database),
		arrived: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	service := lfs.NewService(nil, objectLookup, nil, pendingStore, recorder, nil)
	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- service.Verify(ctx, oid, 0) }()
	}
	for range 2 {
		select {
		case <-objectLookup.arrived:
		case <-time.After(2 * time.Second):
			close(objectLookup.release)
			t.Fatal("concurrent verifiers did not both observe the object as missing")
		}
	}
	close(objectLookup.release)
	for range 2 {
		select {
		case <-pendingStore.arrived:
		case <-time.After(2 * time.Second):
			close(pendingStore.release)
			t.Fatal("concurrent verifiers did not both reach pending consumption")
		}
	}
	close(pendingStore.release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Verify() error = %v", err)
		}
	}
	if got := recorder.recordedIDs(); len(got) != 0 {
		t.Fatalf("Verify recorded a second upload event: %v", got)
	}
	usage, err := database.GetFileUsage(ctx, canonicalID)
	if err != nil {
		t.Fatalf("canonical object upload usage = %+v, %v, want one event", usage, err)
	}
	gotUploads := int64(0)
	if usage.UploadCount != nil {
		gotUploads = *usage.UploadCount
	}
	if gotUploads != 1 {
		t.Fatalf("canonical object upload count = %d, want one event", gotUploads)
	}
	var orphanedEvents int
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM object_usage_event WHERE object_id = ?`, oid).Scan(&orphanedEvents); err != nil {
		t.Fatalf("count pending OID usage events: %v", err)
	}
	if orphanedEvents != 0 {
		t.Fatalf("pending OID upload events = %d, want reassigned and flushed", orphanedEvents)
	}
	if err := service.Verify(ctx, oid, 0); err != nil {
		t.Fatalf("completed Verify() retry error = %v", err)
	}
	usage, err = database.GetFileUsage(ctx, canonicalID)
	if err != nil {
		t.Fatalf("canonical object upload usage after retry = %+v, %v, want one event", usage, err)
	}
	gotUploads = 0
	if usage.UploadCount != nil {
		gotUploads = *usage.UploadCount
	}
	if gotUploads != 1 {
		t.Fatalf("canonical object upload count after retry = %d, want one event", gotUploads)
	}
	if got := recorder.recordedIDs(); len(got) != 0 {
		t.Fatalf("Verify retry recorded an upload event: %v", got)
	}
}

func seedDurableUploadReceipt(t *testing.T, database *store.Store, oid string, size int64, location string) {
	t.Helper()
	now := time.Now().UTC()
	if err := database.SaveLFSUploadReceipt(context.Background(), lfs.UploadReceipt{
		OID:         oid,
		Size:        size,
		SHA256:      oid,
		StorageURL:  location,
		CompletedAt: now,
		ExpiresAt:   now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("save upload receipt: %v", err)
	}
}
