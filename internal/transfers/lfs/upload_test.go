package lfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/internal/objects"
)

type lfsUploadAccountingSpy struct {
	events *[]string
	object string
	err    error
}

func (s *lfsUploadAccountingSpy) RecordFileUpload(_ context.Context, objectID string) error {
	*s.events = append(*s.events, "account")
	s.object = objectID
	return s.err
}

type lfsMetadataObjectSpy struct {
	events              *[]string
	getErr              error
	object              *drs.DrsObject
	registered          []drs.DrsObject
	registerErr         error
	objectAfterRegister bool
}

func (s *lfsMetadataObjectSpy) GetObject(_ context.Context, _, _ string) (*drs.DrsObject, error) {
	*s.events = append(*s.events, "get")
	return s.object, s.getErr
}

func (s *lfsMetadataObjectSpy) RegisterObjects(_ context.Context, records []drs.DrsObject) ([]drs.DrsObject, error) {
	*s.events = append(*s.events, "register")
	s.registered = append([]drs.DrsObject(nil), records...)
	if s.objectAfterRegister && len(records) > 0 {
		s.object = &records[0]
		s.getErr = nil
	}
	if s.registerErr != nil {
		return nil, s.registerErr
	}
	return records, nil
}

func (s *lfsMetadataObjectSpy) RegisterObjectsIfPending(ctx context.Context, records []drs.DrsObject, _ objects.PendingRegistration) ([]drs.DrsObject, error) {
	return s.RegisterObjects(ctx, records)
}

type metadataPendingSpy struct {
	events           *[]string
	entry            *PendingMetadata
	replacement      *PendingMetadata
	getErr           error
	consumeErr       error
	uploadReceipt    *UploadReceipt
	uploadReceiptErr error
}

func (s *metadataPendingSpy) SavePendingMetadata(context.Context, []PendingMetadata) error {
	return nil
}

func (s *metadataPendingSpy) GetPendingMetadata(context.Context, string) (*PendingMetadata, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.entry == nil {
		return nil, fmt.Errorf("%w: pending metadata not found", errorapi.ErrNotFound)
	}
	return s.entry, nil
}

func (s *metadataPendingSpy) ConsumePendingMetadata(_ context.Context, _ PendingMetadata) (bool, error) {
	*s.events = append(*s.events, "consume")
	if s.consumeErr != nil {
		return false, s.consumeErr
	}
	if s.replacement != nil {
		s.entry = s.replacement
		s.replacement = nil
		return false, nil
	}
	s.entry = nil
	return true, nil
}

func (s *metadataPendingSpy) SaveLFSUploadReceipt(_ context.Context, receipt UploadReceipt) error {
	s.uploadReceipt = &receipt
	return nil
}

func (s *metadataPendingSpy) GetLFSUploadReceipt(_ context.Context, oid string) (*UploadReceipt, error) {
	if s.uploadReceiptErr != nil {
		return nil, s.uploadReceiptErr
	}
	if s.uploadReceipt == nil || s.uploadReceipt.OID != oid {
		return nil, fmt.Errorf("%w: completed LFS upload not found", errorapi.ErrNotFound)
	}
	return s.uploadReceipt, nil
}

func seedLFSUploadReceipt(pending *metadataPendingSpy, oid string, size int64, storageURL ...string) {
	now := time.Now().UTC()
	target := "s3://bucket/" + oid
	if len(storageURL) > 0 {
		target = storageURL[0]
	}
	pending.uploadReceipt = &UploadReceipt{
		OID:         oid,
		Size:        size,
		SHA256:      oid,
		StorageURL:  target,
		CompletedAt: now,
		ExpiresAt:   now.Add(time.Hour),
	}
}

func TestLFSVerifyExistingObjectIsIdempotentWithoutPendingCandidate(t *testing.T) {
	events := make([]string, 0, 1)
	sha := strings.Repeat("1", 64)
	service := NewService(nil,
		&lfsMetadataObjectSpy{events: &events, object: &drs.DrsObject{Id: "record"}},
		nil,
		&metadataPendingSpy{events: &events},
		&lfsUploadAccountingSpy{events: &events},
		nil,
	)

	if err := service.Verify(context.Background(), sha, 0); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if strings.Join(events, ",") != "get" {
		t.Fatalf("events = %v, want existing object lookup only", events)
	}
}

func TestLFSVerifyRetainsPendingMetadataWhenRecordedSizeMismatches(t *testing.T) {
	events := make([]string, 0, 4)
	sha := strings.Repeat("e", 64)
	typeName := "s3"
	url := "s3://bucket/" + sha
	methods := []lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}}
	candidate := lfsapi.DrsObjectCandidate{
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: sha}},
		AccessMethods: &methods,
		Size:          func() *int64 { size := int64(7); return &size }(),
	}
	pending := &metadataPendingSpy{
		events: &events,
		entry:  &PendingMetadata{OID: sha, Candidate: candidate},
	}
	seedLFSUploadReceipt(pending, sha, 8)
	objectsPort := &lfsMetadataObjectSpy{events: &events, getErr: errorapi.ErrNotFound}
	accounting := &lfsUploadAccountingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	err := service.Verify(context.Background(), sha, 8)
	var candidateErr *MetadataCandidateError
	if !errors.As(err, &candidateErr) {
		t.Fatalf("Verify() error = %v, want metadata candidate error", err)
	}
	if !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("Verify() error = %v, want size mismatch", err)
	}
	if len(objectsPort.registered) != 0 || accounting.object != "" || pending.entry == nil {
		t.Fatalf("size mismatch side effects = registered=%v accounted=%q pending=%v", objectsPort.registered, accounting.object, pending.entry)
	}
	if strings.Join(events, ",") != "get" {
		t.Fatalf("size mismatch events = %v, want [get]", events)
	}
}

func TestLFSVerifyRejectsExistingObjectSizeMismatchBeforePendingMutation(t *testing.T) {
	events := make([]string, 0, 4)
	sha := strings.Repeat("f", 64)
	pending := &metadataPendingSpy{
		events: &events,
		entry:  &PendingMetadata{OID: sha},
	}
	objectsPort := &lfsMetadataObjectSpy{
		events: &events,
		object: &drs.DrsObject{Id: "record", Size: 7},
	}
	accounting := &lfsUploadAccountingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	err := service.Verify(context.Background(), sha, 8)
	var candidateErr *MetadataCandidateError
	if !errors.As(err, &candidateErr) || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("Verify() error = %v, want metadata size mismatch", err)
	}
	if len(objectsPort.registered) != 0 || accounting.object != "" || pending.entry == nil {
		t.Fatalf("existing mismatch side effects = registered=%v accounted=%q pending=%v", objectsPort.registered, accounting.object, pending.entry)
	}
	if strings.Join(events, ",") != "get" {
		t.Fatalf("existing mismatch events = %v, want [get]", events)
	}
}

func TestLFSMetadataWorkflowConsumesAndRegistersWithoutRecountingUpload(t *testing.T) {
	events := make([]string, 0, 5)
	sha := strings.Repeat("a", 64)
	typeName := "s3"
	url := "s3://bucket/" + sha
	methods := []lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}}
	candidate := lfsapi.DrsObjectCandidate{
		Aliases:       &[]string{"id:" + sha},
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: sha}},
		AccessMethods: &methods,
	}
	pending := &metadataPendingSpy{
		events: &events,
		entry:  &PendingMetadata{OID: sha, Candidate: candidate},
	}
	seedLFSUploadReceipt(pending, sha, 0)
	objectsPort := &lfsMetadataObjectSpy{events: &events, getErr: errorapi.ErrNotFound}
	accounting := &lfsUploadAccountingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	if err := service.Verify(context.Background(), sha, 0); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	wantEvents := []string{"get", "register", "consume"}
	if strings.Join(events, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	if len(objectsPort.registered) != 1 {
		t.Fatalf("registered records = %+v", objectsPort.registered)
	}
	if accounting.object != "" {
		t.Fatalf("Verify recorded a second upload event for %q", accounting.object)
	}
	if pending.entry != nil {
		t.Fatal("pending metadata was not consumed")
	}
}

func TestLFSMetadataWorkflowRetainsPendingMetadataWhenRegistrationFails(t *testing.T) {
	events := make([]string, 0, 8)
	sha := strings.Repeat("b", 64)
	typeName := "s3"
	url := "s3://bucket/" + sha
	methods := []lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}}
	candidate := lfsapi.DrsObjectCandidate{
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: sha}},
		AccessMethods: &methods,
	}
	pending := &metadataPendingSpy{
		events: &events,
		entry:  &PendingMetadata{OID: sha, Candidate: candidate},
	}
	seedLFSUploadReceipt(pending, sha, 0)
	registerErr := fmt.Errorf("registration unavailable")
	objectsPort := &lfsMetadataObjectSpy{events: &events, getErr: errorapi.ErrNotFound, registerErr: registerErr}
	accounting := &lfsUploadAccountingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	if err := service.Verify(context.Background(), sha, 0); err != registerErr {
		t.Fatalf("first Verify() error = %v, want %v", err, registerErr)
	}
	if pending.entry == nil {
		t.Fatal("pending metadata was consumed after registration failure")
	}
	if strings.Join(events, ",") != "get,register" {
		t.Fatalf("first Verify() events = %v, want [get register]", events)
	}

	objectsPort.registerErr = nil
	if err := service.Verify(context.Background(), sha, 0); err != nil {
		t.Fatalf("retry Verify() error = %v", err)
	}
	if pending.entry != nil {
		t.Fatal("pending metadata was not consumed after successful retry")
	}
	if strings.Join(events, ",") != "get,register,get,register,consume" {
		t.Fatalf("retry Verify() events = %v", events)
	}
	if accounting.object != "" {
		t.Fatalf("retry Verify recorded a second upload event for %q", accounting.object)
	}
}

func TestLFSVerifyDoesNotReregisterAnExistingObject(t *testing.T) {
	events := make([]string, 0, 8)
	sha := strings.Repeat("d", 64)
	typeName := "s3"
	candidate := func(url string) lfsapi.DrsObjectCandidate {
		return lfsapi.DrsObjectCandidate{
			Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: sha}},
			AccessMethods: &[]lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}},
		}
	}
	oldURL := "s3://bucket/old/" + sha
	replacementURL := "s3://bucket/replacement/" + sha
	pending := &metadataPendingSpy{
		events: &events,
		entry: &PendingMetadata{
			OID:       sha,
			Candidate: candidate(oldURL),
		},
		replacement: &PendingMetadata{
			OID:       sha,
			Candidate: candidate(replacementURL),
		},
	}
	seedLFSUploadReceipt(pending, sha, 0, oldURL)
	objectsPort := &lfsMetadataObjectSpy{
		events:              &events,
		getErr:              errorapi.ErrNotFound,
		objectAfterRegister: true,
	}
	accounting := &lfsUploadAccountingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	if err := service.Verify(context.Background(), sha, 0); err != nil {
		t.Fatalf("first Verify() error = %v", err)
	}
	if err := service.Verify(context.Background(), sha, 0); err == nil || !strings.Contains(err.Error(), "does not match staged metadata") {
		t.Fatalf("replacement Verify() error = %v, want staged-target mismatch", err)
	}

	if len(objectsPort.registered) != 1 {
		t.Fatalf("registered records = %d, want one initial registration", len(objectsPort.registered))
	}
	if methods := objectsPort.registered[0].AccessMethods; methods == nil || len(*methods) != 1 || (*methods)[0].AccessUrl == nil || (*methods)[0].AccessUrl.Url != oldURL {
		t.Fatalf("initial registration used unexpected metadata: %+v", objectsPort.registered[0])
	}
	if pending.entry == nil || pending.entry.Candidate.AccessMethods == nil || (*pending.entry.Candidate.AccessMethods)[0].AccessUrl.Url == nil || *(*pending.entry.Candidate.AccessMethods)[0].AccessUrl.Url != replacementURL {
		t.Fatal("verification of an existing object unexpectedly consumed replacement metadata")
	}
	if strings.Join(events, ",") != "get,register,consume,get" {
		t.Fatalf("events = %v, want existing-object candidate target validation after lookup", events)
	}
}

func TestLFSMetadataWorkflowReturnsConsumptionFailureAfterRegistration(t *testing.T) {
	events := make([]string, 0, 5)
	sha := strings.Repeat("c", 64)
	typeName := "s3"
	url := "s3://bucket/" + sha
	methods := []lfsapi.AccessMethod{{Type: &typeName, AccessUrl: &lfsapi.AccessMethodAccessUrl{Url: &url}}}
	candidate := lfsapi.DrsObjectCandidate{
		Checksums:     &[]lfsapi.Checksum{{Type: "sha256", Checksum: sha}},
		AccessMethods: &methods,
	}
	consumeErr := fmt.Errorf("pending database unavailable")
	pending := &metadataPendingSpy{
		events:     &events,
		entry:      &PendingMetadata{OID: sha, Candidate: candidate},
		consumeErr: consumeErr,
	}
	seedLFSUploadReceipt(pending, sha, 0)
	objectsPort := &lfsMetadataObjectSpy{events: &events, getErr: errorapi.ErrNotFound}
	accounting := &lfsUploadAccountingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	if err := service.Verify(context.Background(), sha, 0); err != consumeErr {
		t.Fatalf("Verify() error = %v, want %v", err, consumeErr)
	}
	if len(objectsPort.registered) != 1 {
		t.Fatalf("registered records = %d, want 1", len(objectsPort.registered))
	}
	if accounting.object != "" {
		t.Fatalf("accounting ran after consumption failure for %q", accounting.object)
	}
	if strings.Join(events, ",") != "get,register,consume" {
		t.Fatalf("events = %v, want [get register consume]", events)
	}
}

func TestLFSMetadataWorkflowExistingObjectWithoutPendingMetadataIsAlreadyVerified(t *testing.T) {
	events := make([]string, 0, 2)
	object := &drs.DrsObject{Id: "existing"}
	objectsPort := &lfsMetadataObjectSpy{events: &events, object: object}
	accounting := &lfsUploadAccountingSpy{events: &events}
	pending := &metadataPendingSpy{events: &events}
	service := NewService(nil, objectsPort, nil, pending, accounting, nil)

	if err := service.Verify(context.Background(), strings.Repeat("2", 64), 0); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	wantEvents := []string{"get"}
	if strings.Join(events, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
	if accounting.object != "" {
		t.Fatalf("unexpected accounting for already verified object %q", accounting.object)
	}
}
