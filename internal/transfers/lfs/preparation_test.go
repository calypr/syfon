package lfs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
)

type lfsPreparationObjectSpy struct {
	calls  []string
	object *drs.DrsObject
	getErr error
}

func (s *lfsPreparationObjectSpy) GetObject(_ context.Context, _, method string) (*drs.DrsObject, error) {
	s.calls = append(s.calls, "get:"+method)
	return s.object, s.getErr
}

func (s *lfsPreparationObjectSpy) RegisterObjects(context.Context, []drs.DrsObject) ([]drs.DrsObject, error) {
	return nil, nil
}

func (s *lfsPreparationObjectSpy) RegisterObjectsIfPending(context.Context, []drs.DrsObject, objects.PendingRegistration) ([]drs.DrsObject, error) {
	return nil, nil
}

type lfsPreparationCredentialsSpy struct {
	credentials []buckets.CredentialMetadata
	err         error
	calls       int
}

func (s *lfsPreparationCredentialsSpy) ListCredentialMetadata(context.Context) ([]buckets.CredentialMetadata, error) {
	s.calls++
	return s.credentials, s.err
}

func TestLFSPreparationWorkflowPreservesUploadPreflightAndSizeRules(t *testing.T) {
	objectsPort := &lfsPreparationObjectSpy{getErr: fmt.Errorf("%w: missing", errorapi.ErrNotFound)}
	credentials := &lfsPreparationCredentialsSpy{credentials: []buckets.CredentialMetadata{{Bucket: "bucket"}}}
	service := NewService(transfers.NewService(transfers.Dependencies{}), objectsPort, credentials, nil, nil, nil)

	result, err := service.PrepareUpload(context.Background(), "oid", -3)
	if err != nil {
		t.Fatalf("PrepareUpload() error = %v", err)
	}
	if result.Existing || result.Size != 0 {
		t.Fatalf("PrepareUpload() result = %+v", result)
	}
	if !reflect.DeepEqual(objectsPort.calls, []string{"get:read"}) {
		t.Fatalf("preflight calls = %v", objectsPort.calls)
	}
	if credentials.calls != 1 {
		t.Fatalf("credential calls = %d", credentials.calls)
	}
}

func TestLFSFirstConfiguredBucketUsesFirstMetadataEntry(t *testing.T) {
	lookupErr := errors.New("metadata unavailable")
	for _, test := range []struct {
		name        string
		credentials []buckets.CredentialMetadata
		err         error
		wantBucket  string
		wantErr     error
	}{
		{name: "first repository bucket", credentials: []buckets.CredentialMetadata{{Bucket: " first "}, {Bucket: "second"}}, wantBucket: "first"},
		{name: "blank first remains unconfigured", credentials: []buckets.CredentialMetadata{{Bucket: " "}, {Bucket: "second"}}, wantErr: errorapi.ErrBucketNotConfigured},
		{name: "empty list", wantErr: errorapi.ErrBucketNotConfigured},
		{name: "metadata failure", err: lookupErr, wantErr: lookupErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			credentials := &lfsPreparationCredentialsSpy{credentials: test.credentials, err: test.err}
			service := NewService(nil, nil, credentials, nil, nil, nil)
			bucket, err := service.firstConfiguredBucket(context.Background())
			if bucket != test.wantBucket || !errors.Is(err, test.wantErr) || credentials.calls != 1 {
				t.Fatalf("bucket=%q error=%v calls=%d", bucket, err, credentials.calls)
			}
		})
	}
}

func TestLFSPrepareUploadSkipsMetadataForExistingOrDeniedObject(t *testing.T) {
	credentials := &lfsPreparationCredentialsSpy{credentials: []buckets.CredentialMetadata{{Bucket: "bucket"}}}
	existing := &lfsPreparationObjectSpy{object: &drs.DrsObject{Id: "existing", Size: 17}}
	service := NewService(nil, existing, credentials, nil, nil, nil)
	prepared, err := service.PrepareUpload(context.Background(), "existing", 5)
	if err != nil || !prepared.Existing || prepared.Size != 17 || credentials.calls != 0 {
		t.Fatalf("existing preparation=%+v error=%v metadata calls=%d", prepared, err, credentials.calls)
	}
	missing := &lfsPreparationObjectSpy{getErr: errorapi.ErrObjectNotFound}
	service = NewService(nil, missing, credentials, nil, nil, nil)
	session := access.NewSession("gen3")
	session.AuthHeaderPresent = true
	session.SetAuthorizations(nil, map[string]map[string]bool{}, true)
	ctx := access.WithSession(context.Background(), session)
	_, err = service.PrepareUpload(ctx, "missing", 5)
	if !errors.Is(err, errorapi.ErrAccessDenied) || credentials.calls != 0 {
		t.Fatalf("denied preparation error=%v metadata calls=%d", err, credentials.calls)
	}
}
