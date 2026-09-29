package lfs

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
)

type batchMetadataObjectPort struct {
	objects map[string]*drs.DrsObject
	errors  map[string]error
	onGet   func(string)
	getOIDs []string
}

func (p *batchMetadataObjectPort) GetObject(_ context.Context, oid, _ string) (*drs.DrsObject, error) {
	p.getOIDs = append(p.getOIDs, oid)
	if p.onGet != nil {
		p.onGet(oid)
	}
	if err := p.errors[oid]; err != nil {
		return nil, err
	}
	if object := p.objects[oid]; object != nil {
		return object, nil
	}
	return nil, errorapi.ErrObjectNotFound
}

func (*batchMetadataObjectPort) RegisterObjectsIfPending(context.Context, []drs.DrsObject, objects.PendingRegistration) ([]drs.DrsObject, error) {
	return nil, nil
}

func TestBatchUploadReadsMetadataOnceOnlyForNewAuthorizedObjects(t *testing.T) {
	lookup := &lfsPreparationCredentialsSpy{credentials: []buckets.CredentialMetadata{{Bucket: "first"}}}
	objectPort := &batchMetadataObjectPort{
		objects: map[string]*drs.DrsObject{"existing": {Id: "existing", Size: 17}},
		errors:  map[string]error{"denied": errorapi.ErrAccessDenied},
	}
	service := NewService(nil, objectPort, lookup, nil, nil, nil)

	empty, err := service.Batch(context.Background(), BatchRequest{Operation: "upload"})
	if err != nil || len(empty.Objects) != 0 || lookup.calls != 0 {
		t.Fatalf("empty batch=%+v error=%v metadata calls=%d", empty, err, lookup.calls)
	}
	request := BatchRequest{Operation: "upload", Objects: []BatchObject{
		{OID: "existing", Size: 4}, {OID: "denied", Size: 9},
		{OID: "new-a", Size: -3}, {OID: "new-b", Size: 8},
	}}
	result, err := service.Batch(context.Background(), request)
	if err != nil {
		t.Fatalf("Batch() error = %v", err)
	}
	if lookup.calls != 1 || !reflect.DeepEqual(objectPort.getOIDs, []string{"existing", "denied", "new-a", "new-b"}) {
		t.Fatalf("metadata calls=%d object lookups=%v", lookup.calls, objectPort.getOIDs)
	}
	if got := result.Objects; len(got) != 4 ||
		got[0].OID != "existing" || !got[0].Existing || got[0].Size != 17 || got[0].Err != nil ||
		got[1].OID != "denied" || got[1].Size != 9 || !errors.Is(got[1].Err, errorapi.ErrAccessDenied) ||
		got[2].OID != "new-a" || got[2].Existing || got[2].Size != 0 || got[2].Err != nil ||
		got[3].OID != "new-b" || got[3].Existing || got[3].Size != 8 || got[3].Err != nil {
		t.Fatalf("ordered batch results = %+v", got)
	}

	lookup.calls = 0
	objectPort.getOIDs = nil
	_, err = service.Batch(context.Background(), BatchRequest{Operation: "upload", Objects: []BatchObject{{OID: "existing"}}})
	if err != nil || lookup.calls != 0 {
		t.Fatalf("existing-only batch error=%v metadata calls=%d", err, lookup.calls)
	}

	lookup.calls = 0
	deniedSession := access.NewSession("gen3")
	deniedSession.AuthHeaderPresent = true
	deniedSession.SetAuthorizations(nil, map[string]map[string]bool{}, true)
	ctx := access.WithSession(context.Background(), deniedSession)
	result, err = service.Batch(ctx, BatchRequest{Operation: "upload", Objects: []BatchObject{{OID: "new-a", Size: 6}}})
	if err != nil || len(result.Objects) != 1 || !errors.Is(result.Objects[0].Err, errorapi.ErrAccessDenied) || lookup.calls != 0 {
		t.Fatalf("authorization result=%+v error=%v metadata calls=%d", result, err, lookup.calls)
	}
}

func TestBatchUploadRefreshesMetadataAcrossOperations(t *testing.T) {
	lookup := &lfsPreparationCredentialsSpy{credentials: []buckets.CredentialMetadata{{Bucket: "first"}, {Bucket: "second"}}}
	service := NewService(nil, &batchMetadataObjectPort{}, lookup, nil, nil, nil)
	request := BatchRequest{Operation: "upload", Objects: []BatchObject{{OID: "one"}, {OID: "two"}}}
	result, err := service.Batch(context.Background(), request)
	if err != nil || lookup.calls != 1 || result.Objects[0].Err != nil || result.Objects[1].Err != nil {
		t.Fatalf("first batch=%+v error=%v metadata calls=%d", result, err, lookup.calls)
	}

	lookup.credentials = []buckets.CredentialMetadata{{Bucket: " "}, {Bucket: "second"}}
	result, err = service.Batch(context.Background(), request)
	if err != nil || lookup.calls != 2 || !errors.Is(result.Objects[0].Err, errorapi.ErrBucketNotConfigured) || !errors.Is(result.Objects[1].Err, errorapi.ErrBucketNotConfigured) {
		t.Fatalf("rotated batch=%+v error=%v metadata calls=%d", result, err, lookup.calls)
	}

	lookup.credentials = []buckets.CredentialMetadata{{Bucket: "third"}}
	prepared, err := service.PrepareUpload(context.Background(), "direct", 7)
	if err != nil || prepared.Size != 7 || lookup.calls != 3 {
		t.Fatalf("direct preparation=%+v error=%v metadata calls=%d", prepared, err, lookup.calls)
	}
	lookup.err = errors.New("metadata unavailable")
	result, err = service.Batch(context.Background(), request)
	if err != nil || lookup.calls != 4 || !errors.Is(result.Objects[0].Err, lookup.err) || !errors.Is(result.Objects[1].Err, lookup.err) {
		t.Fatalf("failed batch=%+v error=%v metadata calls=%d", result, err, lookup.calls)
	}
	lookup.err = nil
	result, err = service.Batch(context.Background(), request)
	if err != nil || lookup.calls != 5 || result.Objects[0].Err != nil || result.Objects[1].Err != nil {
		t.Fatalf("retried batch=%+v error=%v metadata calls=%d", result, err, lookup.calls)
	}
}

func TestBatchUploadChecksCancellationAfterMetadataWasLoaded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lookup := &lfsPreparationCredentialsSpy{credentials: []buckets.CredentialMetadata{{Bucket: "bucket"}}}
	objectPort := &batchMetadataObjectPort{onGet: func(oid string) {
		if oid == "second" {
			cancel()
		}
	}}
	service := NewService(nil, objectPort, lookup, nil, nil, nil)
	result, err := service.Batch(ctx, BatchRequest{Operation: "upload", Objects: []BatchObject{{OID: "first"}, {OID: "second"}}})
	if err != nil || lookup.calls != 1 || result.Objects[0].Err != nil || !errors.Is(result.Objects[1].Err, context.Canceled) {
		t.Fatalf("canceled batch=%+v error=%v metadata calls=%d", result, err, lookup.calls)
	}
}
