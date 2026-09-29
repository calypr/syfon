package lfs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/sqlite"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/transfers/lfs"
)

const uploadTestPartSize = 64 * 1024 * 1024

type uploadMultipartSpy struct {
	events     []string
	partNumber []int32
	completed  storage.CompleteMultipartRequest
}

func (s *uploadMultipartSpy) Sign(context.Context, storage.SignRequest) (storage.SignedAccess, error) {
	return storage.SignedAccess{Location: "https://provider.invalid/object"}, nil
}

func (s *uploadMultipartSpy) BeginMultipart(_ context.Context, request storage.BeginMultipartRequest) (storage.UploadID, error) {
	s.events = append(s.events, "begin")
	if request.Target.PhysicalBucket != "bucket" || request.Target.Key != "object" {
		return "", fmt.Errorf("unexpected target: %+v", request.Target)
	}
	return "opaque-upload-id", nil
}

func (s *uploadMultipartSpy) SignMultipartPart(_ context.Context, request storage.MultipartPartRequest) (storage.SignedAccess, error) {
	s.events = append(s.events, "sign")
	s.partNumber = append(s.partNumber, request.PartNumber)
	return storage.SignedAccess{Location: "https://provider.invalid/part"}, nil
}

func (s *uploadMultipartSpy) CompleteMultipart(_ context.Context, request storage.CompleteMultipartRequest) error {
	s.events = append(s.events, "complete")
	s.completed = request
	return nil
}

type uploadObjectSpy struct{}

func (uploadObjectSpy) GetObject(context.Context, string, string) (*drs.DrsObject, error) {
	url := "s3://bucket/object"
	methods := []drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: url}}}
	return &drs.DrsObject{Id: "record", AccessMethods: &methods}, nil
}

func (uploadObjectSpy) GetObjectsByChecksums(context.Context, []string, string) (map[string][]drs.DrsObject, error) {
	return nil, nil
}

func (uploadObjectSpy) RegisterObjects(context.Context, []drs.DrsObject) ([]drs.DrsObject, error) {
	return nil, nil
}

func (uploadObjectSpy) RegisterObjectsIfPending(context.Context, []drs.DrsObject, objects.PendingRegistration) ([]drs.DrsObject, error) {
	return nil, nil
}

type uploadAccountingSpy struct {
	events *[]string
	object string
}

func (s *uploadAccountingSpy) RecordFileUpload(_ context.Context, objectID string) error {
	*s.events = append(*s.events, "account")
	s.object = objectID
	return nil
}

func newUploadTestTransfer(t *testing.T, storagePort *uploadMultipartSpy) (*transfers.Service, lfs.PendingStore) {
	t.Helper()
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return transfers.NewService(transfers.Dependencies{Objects: uploadObjectSpy{}, Storage: storagePort, MultipartSessions: database}), database
}

func TestLFSUploadWorkflowPreservesPartSizeOrderAndAccountingOrder(t *testing.T) {
	multipart := &uploadMultipartSpy{events: make([]string, 0, 8)}
	accounting := &uploadAccountingSpy{events: &multipart.events}
	var partLengths []int
	transfer, pending := newUploadTestTransfer(t, multipart)
	service := lfs.NewService(transfer, uploadObjectSpy{}, nil, pending, accounting, func(_ context.Context, _ string, content []byte) (string, error) {
		multipart.events = append(multipart.events, "upload")
		partLengths = append(partLengths, len(content))
		return fmt.Sprintf("etag-%d", len(partLengths)), nil
	})

	content := bytes.Repeat([]byte{'x'}, uploadTestPartSize+1)
	digest := sha256.Sum256(content)
	oid := hex.EncodeToString(digest[:])
	body := bytes.NewReader(content)
	if err := service.UploadProxy(context.Background(), oid, body); err != nil {
		t.Fatalf("UploadProxy() error = %v", err)
	}
	wantEvents := []string{"begin", "sign", "upload", "sign", "upload", "complete", "account"}
	if strings.Join(multipart.events, ",") != strings.Join(wantEvents, ",") {
		t.Fatalf("events = %v, want %v", multipart.events, wantEvents)
	}
	if len(partLengths) != 2 || partLengths[0] != uploadTestPartSize || partLengths[1] != 1 {
		t.Fatalf("part lengths = %v", partLengths)
	}
	if len(multipart.partNumber) != 2 || multipart.partNumber[0] != 1 || multipart.partNumber[1] != 2 {
		t.Fatalf("part numbers = %v", multipart.partNumber)
	}
	if len(multipart.completed.Parts) != 2 || multipart.completed.Parts[0].ETag != "etag-1" || multipart.completed.Parts[1].ETag != "etag-2" {
		t.Fatalf("completed parts = %+v", multipart.completed.Parts)
	}
	if accounting.object != "record" {
		t.Fatalf("accounted object = %q", accounting.object)
	}
}

func TestLFSUploadEmptyBodyUsesOneZeroBytePart(t *testing.T) {
	multipart := &uploadMultipartSpy{}
	accounting := &uploadAccountingSpy{events: &multipart.events}
	var uploaded []byte
	transfer, pending := newUploadTestTransfer(t, multipart)
	service := lfs.NewService(transfer, uploadObjectSpy{}, nil, pending, accounting, func(_ context.Context, _ string, content []byte) (string, error) {
		uploaded = append([]byte(nil), content...)
		return "empty-etag", nil
	})

	digest := sha256.Sum256(nil)
	oid := hex.EncodeToString(digest[:])
	if err := service.UploadProxy(context.Background(), oid, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	if len(uploaded) != 0 || len(multipart.partNumber) != 1 || multipart.partNumber[0] != 1 {
		t.Fatalf("empty upload parts = %v bytes=%d", multipart.partNumber, len(uploaded))
	}
	if len(multipart.completed.Parts) != 1 || multipart.completed.Parts[0].ETag != "empty-etag" {
		t.Fatalf("empty completion = %+v", multipart.completed.Parts)
	}
}
