package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

const payloadSHA256 = "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"

type completedLocationBackend struct{ uploaderStub }

func (b *completedLocationBackend) MultipartCompleteWithLocation(context.Context, string, string, []transfer.MultipartPart) (string, error) {
	return "s3://physical-bucket/original-project-prefix/payload.bin", nil
}

func TestRegisterLargeFileUsesCompletedMultipartLocation(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "payload.bin")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatal(err)
	}
	size := int64(4.5 * float64(common.GB))
	if err := file.Truncate(size); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATA_CLIENT_CACHE_DIR", filepath.Join(t.TempDir(), "cache"))
	backend := &completedLocationBackend{uploaderStub: uploaderStub{resolveFunc: func(context.Context, string, string, common.FileMetadata, string) (string, error) {
		return "", fmt.Errorf("completed multipart location must not be re-resolved")
	}}}
	metadata := &metadataClientStub{registeredID: "stored-id"}
	obj := &drsapi.DrsObject{Id: "requested-id", Size: size}
	got, err := RegisterFile(context.Background(), backend, metadata, obj, filePath, "physical-bucket")
	if err != nil {
		t.Fatal(err)
	}
	const want = "s3://physical-bucket/original-project-prefix/payload.bin"
	if got.AccessMethods == nil || len(*got.AccessMethods) != 1 {
		t.Fatalf("access methods = %+v", got.AccessMethods)
	}
	if actual := (*got.AccessMethods)[0].AccessUrl.Url; actual != want {
		t.Fatalf("registered URL = %q, want completed location %q", actual, want)
	}
	if len(metadata.requests) != 1 || (*metadata.requests[0].Candidates[0].AccessMethods)[0].AccessUrl.Url != want {
		t.Fatalf("registration request = %+v", metadata.requests)
	}
}

type metadataClientStub struct {
	registeredID string
	registers    int
	updates      int
	requests     []drsapi.RegisterObjectsJSONRequestBody
	object       drsapi.DrsObject
	getErr       error
}

func (m *metadataClientStub) GetObject(context.Context, string) (drsapi.DrsObject, error) {
	if m.getErr != nil {
		return drsapi.DrsObject{}, m.getErr
	}
	if m.object.Id != "" {
		return m.object, nil
	}
	return drsapi.DrsObject{}, errorapi.ErrNotFound
}

func (m *metadataClientStub) RegisterObjects(_ context.Context, req drsapi.RegisterObjectsJSONRequestBody) (drsapi.N201ObjectsCreated, error) {
	m.registers++
	m.requests = append(m.requests, cloneRegisterRequest(req))
	return drsapi.N201ObjectsCreated{
		Objects: []drsapi.DrsObject{{
			Id: m.registeredID,
		}},
	}, nil
}

func (m *metadataClientStub) UpdateObjectAccessMethods(_ context.Context, objectID string, accessMethods []drsapi.AccessMethod) (drsapi.DrsObject, error) {
	m.updates++
	updated := m.object
	if strings.TrimSpace(updated.Id) == "" {
		updated.Id = objectID
	}
	methods := append([]drsapi.AccessMethod(nil), accessMethods...)
	updated.AccessMethods = &methods
	m.object = updated
	return updated, nil
}

func cloneRegisterRequest(req drsapi.RegisterObjectsJSONRequestBody) drsapi.RegisterObjectsJSONRequestBody {
	out := req
	out.Candidates = append([]drsapi.DrsObjectCandidate(nil), req.Candidates...)
	for i := range out.Candidates {
		if out.Candidates[i].ControlledAccess != nil {
			values := append([]string(nil), (*out.Candidates[i].ControlledAccess)...)
			out.Candidates[i].ControlledAccess = &values
		}
		if out.Candidates[i].AccessMethods != nil {
			values := append([]drsapi.AccessMethod(nil), (*out.Candidates[i].AccessMethods)...)
			out.Candidates[i].AccessMethods = &values
		}
	}
	return out
}

func TestUploadUsesTransferEngine(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()
	backend := &uploaderStub{}
	metadata := common.FileMetadata{Authorizations: map[string][]string{"org": {"project"}}}
	if err := Upload(context.Background(), backend, file.Name(), "object-key", "did", "bucket", metadata, false, false); err != nil {
		t.Fatalf("Upload returned error: %v", err)
	}
	if backend.lastResolve.guid != "did" || backend.lastResolve.fileName != "object-key" || backend.lastResolve.bucket != "bucket" {
		t.Fatalf("unexpected upload resolution: %+v", backend.lastResolve)
	}
	if backend.lastUpload.body != "payload" {
		t.Fatalf("uploaded body = %q", backend.lastUpload.body)
	}
}

func TestRegisterFileUploadsUsingRegisteredObjectID(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()

	uploader := &uploaderStub{}
	metadata := &metadataClientStub{registeredID: "server-object-id"}
	name := "payload.bin"
	obj := &drsapi.DrsObject{
		Id:   "requested-object-id",
		Name: &name,
		Size: 7,
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: payloadSHA256,
		}},
	}

	if _, err := RegisterFile(context.Background(), uploader, metadata, obj, file.Name(), "bucket-a"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if uploader.lastResolve.guid != "requested-object-id" {
		t.Fatalf("expected upload URL to use requested object id, got %q", uploader.lastResolve.guid)
	}
	if uploader.lastResolve.fileName != payloadSHA256 {
		t.Fatalf("expected checksum upload key, got %q", uploader.lastResolve.fileName)
	}
}

func TestRegisterFileRejectsUnsupportedMetadataBeforeUpload(t *testing.T) {
	tests := []struct {
		name  string
		field string
		set   func(*drsapi.DrsObject)
	}{
		{
			name:  "contents",
			field: "contents",
			set: func(object *drsapi.DrsObject) {
				object.Contents = &[]drsapi.ContentsObject{{Name: "part.bin"}}
			},
		},
		{
			name:  "mime_type",
			field: "mime_type",
			set: func(object *drsapi.DrsObject) {
				mimeType := "application/octet-stream"
				object.MimeType = &mimeType
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := createTempFileWithData(t, "payload")
			defer file.Close()

			object := &drsapi.DrsObject{
				Id:   "requested-object-id",
				Size: 7,
				Checksums: []drsapi.Checksum{{
					Type: "sha256", Checksum: payloadSHA256,
				}},
			}
			test.set(object)

			backend := &uploaderStub{}
			metadata := &metadataClientStub{registeredID: "server-object-id"}
			_, err := RegisterFile(context.Background(), backend, metadata, object, file.Name(), "bucket-a")
			if err == nil {
				t.Fatalf("RegisterFile accepted unsupported %s metadata", test.field)
			}
			if !errors.Is(err, errorapi.ErrInvalidInput) || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("RegisterFile error = %v, want invalid input naming %q", err, test.field)
			}
			if backend.lastResolve.guid != "" || backend.lastUpload.url != "" || metadata.registers != 0 || metadata.updates != 0 {
				t.Fatalf("unsupported metadata caused side effects: resolve=%+v upload=%+v registers=%d updates=%d", backend.lastResolve, backend.lastUpload, metadata.registers, metadata.updates)
			}
		})
	}
}

func TestRegisterFileUsesSHA256AliasAsCASKey(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()
	const checksum = payloadSHA256
	obj := &drsapi.DrsObject{
		Id:   "requested-object-id",
		Size: 7,
		Checksums: []drsapi.Checksum{{
			Type:     "SHA-256",
			Checksum: checksum,
		}},
	}
	backend := &uploaderStub{}
	metadata := &metadataClientStub{registeredID: "server-object-id"}
	if _, err := RegisterFile(context.Background(), backend, metadata, obj, file.Name(), "bucket-a"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if backend.lastResolve.fileName != checksum {
		t.Fatalf("upload key = %q, want checksum %q", backend.lastResolve.fileName, checksum)
	}
}

func TestRegisterFileRejectsContentChangedSinceSHA256BeforeUpload(t *testing.T) {
	file := createTempFileWithData(t, "before")
	defer file.Close()

	original, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(original)
	checksum := hex.EncodeToString(digest[:])

	if err := os.WriteFile(file.Name(), []byte("change"), 0o600); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != int64(len(original)) {
		t.Fatalf("replacement size = %d, want unchanged size %d", stat.Size(), len(original))
	}

	backend := &uploaderStub{}
	metadata := &metadataClientStub{registeredID: "server-object-id"}
	object := &drsapi.DrsObject{
		Id:   "requested-object-id",
		Size: stat.Size(),
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: checksum,
		}},
	}
	_, err = RegisterFile(context.Background(), backend, metadata, object, file.Name(), "bucket-a")
	if err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("RegisterFile error = %v, want SHA-256 mismatch", err)
	}
	if backend.lastResolve.fileName != "" || backend.lastUpload.url != "" || metadata.registers != 0 || metadata.updates != 0 {
		t.Fatalf("mismatched content caused side effects: resolve=%+v upload=%+v registers=%d updates=%d", backend.lastResolve, backend.lastUpload, metadata.registers, metadata.updates)
	}
}

func TestRegisterFileAcceptsMatchingSHA256(t *testing.T) {
	file := createTempFileWithData(t, "payload")
	defer file.Close()

	content, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	checksum := hex.EncodeToString(digest[:])
	backend := &uploaderStub{}
	metadata := &metadataClientStub{registeredID: "server-object-id"}
	object := &drsapi.DrsObject{
		Id:   "requested-object-id",
		Size: int64(len(content)),
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: checksum,
		}},
	}

	if _, err := RegisterFile(context.Background(), backend, metadata, object, file.Name(), "bucket-a"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if backend.lastResolve.fileName != checksum || backend.lastUpload.body != string(content) {
		t.Fatalf("upload = resolve=%+v body=%q, want checksum key and unchanged content", backend.lastResolve, backend.lastUpload.body)
	}
	if metadata.registers != 1 {
		t.Fatalf("RegisterObjects calls = %d, want 1", metadata.registers)
	}
}

func TestRegisterFileOnlyFallsBackForTypedNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lookupErr  error
		wantErr    bool
		wantLookup error
	}{
		{name: "not found", lookupErr: errorapi.ErrNotFound},
		{name: "wrapped not found", lookupErr: fmt.Errorf("lookup: %w", errorapi.ErrObjectNotFound)},
		{name: "unauthorized", lookupErr: fmt.Errorf("lookup: %w", errorapi.ErrUnauthorized), wantErr: true, wantLookup: errorapi.ErrUnauthorized},
		{name: "transport", lookupErr: errors.New("transport unavailable"), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := createTempFileWithData(t, "payload")
			defer file.Close()
			backend := &uploaderStub{}
			metadata := &metadataClientStub{registeredID: "server-object-id", getErr: test.lookupErr}
			obj := &drsapi.DrsObject{Id: "requested-object-id", Size: 7}

			_, err := RegisterFile(context.Background(), backend, metadata, obj, file.Name(), "bucket-a")
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "failed to look up existing object") {
					t.Fatalf("lookup error = %v, want wrapped lookup failure", err)
				}
				if test.wantLookup != nil && !errors.Is(err, test.wantLookup) {
					t.Fatalf("lookup error = %v, want cause %v", err, test.wantLookup)
				}
				if metadata.registers != 0 {
					t.Fatalf("RegisterObjects calls = %d, want 0", metadata.registers)
				}
				return
			}
			if err != nil {
				t.Fatalf("typed not-found RegisterFile error = %v", err)
			}
			if metadata.registers != 1 {
				t.Fatalf("RegisterObjects calls = %d, want 1", metadata.registers)
			}
		})
	}
}

func TestRegisterFilePreservesScopedRoutingMetadata(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()

	name := "payload.bin"
	controlledAccess := []string{"/organization/syfon/project/e2e"}
	accessMethods := []drsapi.AccessMethod{{
		Type:      "s3",
		AccessUrl: &drsapi.AccessURL{Url: "s3://syfon-e2e-bucket/project-subpath/" + payloadSHA256},
	}}
	obj := &drsapi.DrsObject{
		Id:               "requested-object-id",
		Name:             &name,
		Size:             7,
		ControlledAccess: &controlledAccess,
		AccessMethods:    &accessMethods,
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: payloadSHA256,
		}},
	}

	uploader := &uploaderStub{}
	metadata := &metadataClientStub{
		registeredID: "server-object-id",
		object: drsapi.DrsObject{
			Id:               "server-object-id",
			Name:             &name,
			Size:             7,
			ControlledAccess: &controlledAccess,
			AccessMethods:    &accessMethods,
			Checksums:        obj.Checksums,
		},
	}

	if _, err := RegisterFile(context.Background(), uploader, metadata, obj, file.Name(), "syfon-e2e-bucket"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if len(metadata.requests) != 1 {
		t.Fatalf("expected single final register call, got %d", len(metadata.requests))
	}
	if metadata.updates != 1 {
		t.Fatalf("expected finalized access-method update, got %d", metadata.updates)
	}
	for i, req := range metadata.requests {
		if len(req.Candidates) != 1 {
			t.Fatalf("register call %d expected one candidate, got %d", i, len(req.Candidates))
		}
		candidate := req.Candidates[0]
		if candidate.ControlledAccess == nil || len(*candidate.ControlledAccess) != 1 || (*candidate.ControlledAccess)[0] != controlledAccess[0] {
			t.Fatalf("register call %d did not preserve controlled_access: %#v", i, candidate.ControlledAccess)
		}
		if candidate.AccessMethods == nil || len(*candidate.AccessMethods) == 0 {
			t.Fatalf("register call %d did not preserve access methods: %#v", i, candidate.AccessMethods)
		}
	}
	if obj.ControlledAccess == nil || len(*obj.ControlledAccess) != 1 || (*obj.ControlledAccess)[0] != controlledAccess[0] {
		t.Fatalf("returned object did not preserve controlled_access: %#v", obj.ControlledAccess)
	}
}

func TestRegisterFilePrefersExplicitControlledAccessOverExistingObject(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()

	name := "payload.bin"
	targetControlledAccess := []string{"/organization/dst/project/copied"}
	sourceControlledAccess := []string{"/organization/src/project/original"}
	accessMethods := []drsapi.AccessMethod{{
		Type:      "s3",
		AccessUrl: &drsapi.AccessURL{Url: "s3://syfon-bucket/original/" + payloadSHA256},
	}}
	obj := &drsapi.DrsObject{
		Id:               "requested-object-id",
		Name:             &name,
		Size:             7,
		ControlledAccess: &targetControlledAccess,
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: payloadSHA256,
		}},
	}

	uploader := &uploaderStub{}
	metadata := &metadataClientStub{
		registeredID: "server-object-id",
		object: drsapi.DrsObject{
			Id:               "server-object-id",
			Name:             &name,
			Size:             7,
			ControlledAccess: &sourceControlledAccess,
			AccessMethods:    &accessMethods,
			Checksums:        obj.Checksums,
		},
	}

	if _, err := RegisterFile(context.Background(), uploader, metadata, obj, file.Name(), "syfon-bucket"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if len(metadata.requests) != 1 {
		t.Fatalf("expected single final register call, got %d", len(metadata.requests))
	}
	candidate := metadata.requests[0].Candidates[0]
	if candidate.ControlledAccess == nil || len(*candidate.ControlledAccess) != 1 || (*candidate.ControlledAccess)[0] != targetControlledAccess[0] {
		t.Fatalf("expected explicit controlled_access to win, got %#v", candidate.ControlledAccess)
	}
	if candidate.AccessMethods == nil || len(*candidate.AccessMethods) == 0 {
		t.Fatalf("expected existing access methods to still be preserved, got %#v", candidate.AccessMethods)
	}
}

func TestRegisterFileSinglePartStreamsProgress(t *testing.T) {
	t.Parallel()

	payload := make([]byte, common.OnProgressThreshold+257)
	for i := range payload {
		payload[i] = 'a'
	}

	file := createTempFileWithData(t, string(payload))
	defer file.Close()
	digest := sha256.Sum256(payload)
	checksum := hex.EncodeToString(digest[:])

	uploader := &uploaderStub{
		uploadFunc: func(_ context.Context, _ string, body io.Reader, _ int64) error {
			buf := make([]byte, 64*1024)
			for {
				_, err := body.Read(buf)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
			}
		},
	}
	metadata := &metadataClientStub{registeredID: "server-object-id"}
	name := "payload.bin"
	obj := &drsapi.DrsObject{
		Id:   "requested-object-id",
		Name: &name,
		Size: int64(len(payload)),
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: checksum,
		}},
	}

	var events []common.ProgressEvent
	ctx := common.WithOid(context.Background(), checksum)
	ctx = common.WithProgress(ctx, func(ev common.ProgressEvent) error {
		events = append(events, ev)
		return nil
	})

	if _, err := RegisterFile(ctx, uploader, metadata, obj, file.Name(), "bucket-a"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected streamed progress events, got %+v", events)
	}

	progressEventsSeen := 0
	for _, ev := range events {
		if ev.Event == "progress" {
			progressEventsSeen++
		}
	}
	if progressEventsSeen < 2 {
		t.Fatalf("expected threshold and finalize progress events, got %+v", events)
	}

	last := events[len(events)-1]
	if last.BytesSoFar != int64(len(payload)) {
		t.Fatalf("final progress bytes = %d, want %d", last.BytesSoFar, len(payload))
	}
}

func TestRegisterFileUsesOneUploadTarget(t *testing.T) {
	t.Parallel()
	file := createTempFileWithData(t, "payload")
	defer file.Close()
	calls := 0
	uploader := &uploaderStub{resolveFunc: func(context.Context, string, string, common.FileMetadata, string) (string, error) {
		calls++
		return fmt.Sprintf("https://upload.example/target-%d", calls), nil
	}}
	metadata := &metadataClientStub{registeredID: "id"}
	obj := &drsapi.DrsObject{Id: "id", Size: 7}
	got, err := RegisterFile(context.Background(), uploader, metadata, obj, file.Name(), "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("resolved upload target %d times", calls)
	}
	if got.AccessMethods == nil || len(*got.AccessMethods) != 1 {
		t.Fatalf("missing access method: %+v", got)
	}
	if target := (*got.AccessMethods)[0].AccessUrl.Url; target != uploader.lastUpload.url || target != "https://upload.example/target-1" {
		t.Fatalf("registered %q but uploaded to %q", target, uploader.lastUpload.url)
	}
}
