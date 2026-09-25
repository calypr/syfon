package copyproject

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/logs"
	"github.com/calypr/syfon/client/services"
	"github.com/spf13/cobra"
)

type copyInternalAPI struct {
	internalapi.ClientWithResponsesInterface
	downloadURLs   map[string]string
	uploadURL      string
	uploadURLCalls int
	uploadFilename string
	getStatus      int
	created        *internalapi.InternalRecord
}

func (f *copyInternalAPI) InternalDownloadWithResponse(_ context.Context, fileID string, _ *internalapi.InternalDownloadParams, _ ...internalapi.RequestEditorFn) (*internalapi.InternalDownloadResponse, error) {
	value := f.downloadURLs[fileID]
	return &internalapi.InternalDownloadResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &internalapi.InternalSignedURL{Url: &value},
	}, nil
}

func (f *copyInternalAPI) InternalUploadURLWithResponse(_ context.Context, _ string, params *internalapi.InternalUploadURLParams, _ ...internalapi.RequestEditorFn) (*internalapi.InternalUploadURLResponse, error) {
	f.uploadURLCalls++
	if params != nil && params.Key != nil {
		f.uploadFilename = *params.Key
	}
	value := f.uploadURL
	return &internalapi.InternalUploadURLResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &internalapi.InternalSignedURL{Url: &value},
	}, nil
}

func (f *copyInternalAPI) InternalGetWithResponse(_ context.Context, _ string, _ ...internalapi.RequestEditorFn) (*internalapi.InternalGetResponse, error) {
	status := f.getStatus
	if status == 0 {
		status = http.StatusNotFound
	}
	return &internalapi.InternalGetResponse{HTTPResponse: &http.Response{StatusCode: status}}, nil
}

func (f *copyInternalAPI) InternalCreateWithResponse(_ context.Context, body internalapi.InternalCreateJSONRequestBody, _ ...internalapi.RequestEditorFn) (*internalapi.InternalCreateResponse, error) {
	created := internalapi.InternalRecord(body)
	f.created = &created
	response := internalapi.InternalRecordResponse{Did: created.Did}
	return &internalapi.InternalCreateResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusCreated},
		JSON201:      &response,
	}, nil
}

type copyDRSAPI struct {
	drsapi.ClientWithResponsesInterface
	objects       map[string]drsapi.DrsObject
	getStatus     map[string]int
	registered    *drsapi.RegisterObjectsJSONRequestBody
	updated       *drsapi.UpdateObjectAccessMethodsJSONRequestBody
	registerCalls int
	updateCalls   int
}

func (f *copyDRSAPI) GetObjectWithResponse(_ context.Context, objectID drsapi.ObjectId, _ *drsapi.GetObjectParams, _ ...drsapi.RequestEditorFn) (*drsapi.GetObjectResponse, error) {
	id := string(objectID)
	status := f.getStatus[id]
	if status == 0 {
		status = http.StatusNotFound
	}
	response := &drsapi.GetObjectResponse{HTTPResponse: &http.Response{StatusCode: status}}
	if object, ok := f.objects[id]; ok {
		response.JSON200 = &object
		response.HTTPResponse.StatusCode = http.StatusOK
	}
	return response, nil
}

func (f *copyDRSAPI) RegisterObjectsWithResponse(_ context.Context, body drsapi.RegisterObjectsJSONRequestBody, _ ...drsapi.RequestEditorFn) (*drsapi.RegisterObjectsResponse, error) {
	request := drsapi.RegisterObjectsJSONRequestBody(body)
	f.registered = &request
	f.registerCalls++
	return &drsapi.RegisterObjectsResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusCreated},
		JSON201:      &drsapi.N201ObjectsCreated{},
	}, nil
}

func (f *copyDRSAPI) UpdateObjectAccessMethodsWithResponse(_ context.Context, objectID string, body drsapi.UpdateObjectAccessMethodsJSONRequestBody, _ ...drsapi.RequestEditorFn) (*drsapi.UpdateObjectAccessMethodsResponse, error) {
	request := drsapi.UpdateObjectAccessMethodsJSONRequestBody(body)
	f.updated = &request
	f.updateCalls++
	object := drsapi.DrsObject{Id: objectID, AccessMethods: &request.AccessMethods}
	return &drsapi.UpdateObjectAccessMethodsResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &object,
	}, nil
}

type copyRecordClient struct {
	data  *services.DataService
	index *services.IndexService
	drs   *services.DRSService
}

func (f *copyRecordClient) Health() *services.HealthService   { return nil }
func (f *copyRecordClient) Data() *services.DataService       { return f.data }
func (f *copyRecordClient) Index() *services.IndexService     { return f.index }
func (f *copyRecordClient) DRS() *services.DRSService         { return f.drs }
func (f *copyRecordClient) Buckets() *services.BucketsService { return nil }
func (f *copyRecordClient) Metrics() *services.MetricsService { return nil }
func (f *copyRecordClient) LFS() *services.LFSService         { return nil }

func TestCopyRecordCopiesBytesAndPublishesDestinationProviderMetadata(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "source.txt")
	targetPath := filepath.Join(t.TempDir(), "nested", "target.bin")
	payload := []byte("record payload")
	if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}

	sourceURL := fileURL(t, sourcePath)
	targetURL := fileURL(t, targetPath)
	sourceInternal := &copyInternalAPI{downloadURLs: map[string]string{"did-copy": sourceURL}}
	targetInternal := &copyInternalAPI{uploadURL: targetURL}
	accessMethods := []drsapi.AccessMethod{{Type: drsapi.AccessMethodType("s3"), AccessUrl: &drsapi.AccessURL{Url: "s3://source/object"}}}
	size := int64(len(payload))
	digest := sha256.Sum256(payload)
	checksum := hex.EncodeToString(digest[:])
	sourceDescription := "Copied project description"
	sourceDRS := &copyDRSAPI{objects: map[string]drsapi.DrsObject{"did-copy": {Id: "did-copy", Size: size, AccessMethods: &accessMethods}}}
	existingMethods := []drsapi.AccessMethod{{Type: "s3", AccessUrl: &drsapi.AccessURL{Url: "s3://target-bucket/existing-copy"}}}
	targetDRS := &copyDRSAPI{objects: map[string]drsapi.DrsObject{"did-copy": {Id: "did-copy", AccessMethods: &existingMethods}}}
	logger := logs.NewGen3Logger(slog.New(slog.NewTextHandler(io.Discard, nil)), "", "syfon")
	sourceData := services.NewDataService(sourceInternal, nil, logger, services.NewDRSService(sourceDRS))
	targetData := services.NewDataService(targetInternal, nil, logger, services.NewDRSService(targetDRS))
	sourceClient := &copyRecordClient{data: sourceData, drs: services.NewDRSService(sourceDRS)}
	targetClient := &copyRecordClient{data: targetData, index: services.NewIndexService(targetInternal), drs: services.NewDRSService(targetDRS)}

	rec := internalapi.InternalRecord{
		Did:           "did-copy",
		Name:          stringPtr("payload.txt"),
		Description:   &sourceDescription,
		Size:          &size,
		Hashes:        &internalapi.HashInfo{"sha256": checksum},
		AccessMethods: &accessMethods,
	}
	cmd := &cobra.Command{}
	var output bytes.Buffer
	cmd.SetOut(&output)
	if err := copyRecord(context.Background(), cmd, sourceClient, targetClient, rec, "target-bucket", "gcs", "s3://target-bucket/organizations/target-org/projects/target-project", "/organization/target-org/project/target-project", 1, 1, t.TempDir()); err != nil {
		t.Fatalf("copyRecord returned error: %v", err)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("ReadFile target: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("target bytes = %q; want %q", got, payload)
	}
	targetObjectURL := "gs://target-bucket/organizations/target-org/projects/target-project/" + checksum
	for _, want := range []string{"[1/1] Copying did-copy", "Downloading did-copy", "Uploading did-copy", targetObjectURL} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("copy output %q does not contain %q", output.String(), want)
		}
	}
	if targetDRS.registerCalls != 1 || targetDRS.registered == nil || len(targetDRS.registered.Candidates) != 1 || targetDRS.registered.Candidates[0].Name == nil || *targetDRS.registered.Candidates[0].Name != "payload.txt" {
		t.Fatalf("registered DRS request = %+v", targetDRS.registered)
	}
	if targetDRS.updateCalls != 1 || targetDRS.updated == nil || len(targetDRS.updated.AccessMethods) != 2 || targetDRS.updated.AccessMethods[0].AccessUrl == nil || targetDRS.updated.AccessMethods[0].AccessUrl.Url != "s3://target-bucket/existing-copy" || targetDRS.updated.AccessMethods[1].Type != drsapi.AccessMethodType("gs") || targetDRS.updated.AccessMethods[1].AccessUrl == nil || targetDRS.updated.AccessMethods[1].AccessUrl.Url != targetObjectURL {
		t.Fatalf("updated DRS request = %+v", targetDRS.updated)
	}
	if targetInternal.created != nil {
		t.Fatalf("copyRecord performed a duplicate index write: %+v", targetInternal.created)
	}
	if candidate := targetDRS.registered.Candidates[0]; candidate.ControlledAccess == nil || len(*candidate.ControlledAccess) != 1 || (*candidate.ControlledAccess)[0] != "/organization/target-org/project/target-project" {
		t.Fatalf("registered scope = %+v", candidate.ControlledAccess)
	}
	if candidate := targetDRS.registered.Candidates[0]; candidate.Description == nil || *candidate.Description != sourceDescription {
		t.Fatalf("registered description = %v; want %q", candidate.Description, sourceDescription)
	}
}

func TestCopyRecordReturnsDownloadErrorForCallerToSkip(t *testing.T) {
	missingTempDir := filepath.Join(t.TempDir(), "does-not-exist")
	cmd := &cobra.Command{}
	var output bytes.Buffer
	cmd.SetOut(&output)
	err := copyRecord(context.Background(), cmd, nil, nil, internalapi.InternalRecord{Did: "did-skip"}, "bucket", "s3", "", "/organization/org/project/project", 2, 3, missingTempDir)
	if err == nil || !strings.Contains(err.Error(), "failed to create temp file") {
		t.Fatalf("copyRecord error = %v; want temp-file error", err)
	}
	if !strings.Contains(output.String(), "[2/3] Copying did-skip (size: 0, name: )") {
		t.Fatalf("copy output = %q", output.String())
	}
}

func TestCopyRecordHashesLegacyFileBeforeUploadAndRegistration(t *testing.T) {
	payload := []byte("legacy record without a SHA-256")
	digest := sha256.Sum256(payload)
	checksum := hex.EncodeToString(digest[:])
	sourcePath := filepath.Join(t.TempDir(), "source.txt")
	targetPath := filepath.Join(t.TempDir(), "target.bin")
	if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}

	accessMethods := []drsapi.AccessMethod{{Type: "s3", AccessUrl: &drsapi.AccessURL{Url: "s3://source-bucket/legacy-key"}}}
	size := int64(len(payload))
	input := &copyInternalAPI{downloadURLs: map[string]string{"did-legacy": fileURL(t, sourcePath)}}
	output := &copyInternalAPI{uploadURL: fileURL(t, targetPath)}
	drs := &copyDRSAPI{}
	logger := logs.NewGen3Logger(slog.New(slog.NewTextHandler(io.Discard, nil)), "", "syfon")
	sourceClient := &copyRecordClient{data: services.NewDataService(input, nil, logger, nil)}
	targetClient := &copyRecordClient{data: services.NewDataService(output, nil, logger, services.NewDRSService(drs)), drs: services.NewDRSService(drs)}
	rec := internalapi.InternalRecord{
		Did:           "did-legacy",
		Name:          stringPtr("legacy.txt"),
		Size:          &size,
		AccessMethods: &accessMethods,
	}

	if err := copyRecord(context.Background(), &cobra.Command{}, sourceClient, targetClient, rec, "target-bucket", "s3", "s3://target-bucket/organizations/target-org/projects/target-project", "/organization/target-org/project/target-project", 1, 1, t.TempDir()); err != nil {
		t.Fatalf("copyRecord returned error: %v", err)
	}
	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("ReadFile target: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("uploaded bytes = %q; want %q", got, payload)
	}
	if output.uploadURLCalls != 1 || output.uploadFilename != checksum {
		t.Fatalf("upload URL calls/name = %d/%q; want 1/%q", output.uploadURLCalls, output.uploadFilename, checksum)
	}
	if drs.registerCalls != 1 || drs.registered == nil || len(drs.registered.Candidates) != 1 || len(drs.registered.Candidates[0].Checksums) != 1 || drs.registered.Candidates[0].Checksums[0].Type != "sha256" || drs.registered.Candidates[0].Checksums[0].Checksum != checksum {
		t.Fatalf("registered checksums = %+v; want computed sha256 %q", drs.registered, checksum)
	}
	wantAccessURL := "s3://target-bucket/organizations/target-org/projects/target-project/" + checksum
	if drs.updated == nil || len(drs.updated.AccessMethods) != 1 || drs.updated.AccessMethods[0].Type != "s3" || drs.updated.AccessMethods[0].AccessUrl == nil || drs.updated.AccessMethods[0].AccessUrl.Url != wantAccessURL {
		t.Fatalf("registered access methods = %+v; want SHA-keyed URL %q", drs.updated, wantAccessURL)
	}
}

func TestCopyRecordRejectsInvalidOrConflictingSHA256BeforeTargetSideEffects(t *testing.T) {
	payload := []byte("record with supplied SHA-256")
	digest := sha256.Sum256(payload)
	conflictingDigest := strings.Repeat("0", sha256.Size*2)
	if conflictingDigest == hex.EncodeToString(digest[:]) {
		conflictingDigest = strings.Repeat("1", sha256.Size*2)
	}
	size := int64(len(payload))
	for _, tc := range []struct {
		name       string
		checksum   string
		wantErrSub string
	}{
		{name: "invalid", checksum: "sha256:not-valid", wantErrSub: "invalid source SHA-256"},
		{name: "conflicting", checksum: conflictingDigest, wantErrSub: "SHA-256 mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "source.txt")
			targetPath := filepath.Join(t.TempDir(), "target.bin")
			if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
				t.Fatalf("WriteFile source: %v", err)
			}
			input := &copyInternalAPI{downloadURLs: map[string]string{"did-mismatch": fileURL(t, sourcePath)}}
			output := &copyInternalAPI{uploadURL: fileURL(t, targetPath)}
			drs := &copyDRSAPI{}
			logger := logs.NewGen3Logger(slog.New(slog.NewTextHandler(io.Discard, nil)), "", "syfon")
			sourceClient := &copyRecordClient{data: services.NewDataService(input, nil, logger, nil)}
			targetClient := &copyRecordClient{data: services.NewDataService(output, nil, logger, services.NewDRSService(drs)), drs: services.NewDRSService(drs)}
			rec := internalapi.InternalRecord{
				Did:    "did-mismatch",
				Size:   &size,
				Hashes: &internalapi.HashInfo{"sha256": tc.checksum},
			}

			err := copyRecord(context.Background(), &cobra.Command{}, sourceClient, targetClient, rec, "target-bucket", "s3", "s3://target-bucket/organizations/target-org/projects/target-project", "/organization/target-org/project/target-project", 1, 1, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("copyRecord error = %v; want %q", err, tc.wantErrSub)
			}
			if output.uploadURLCalls != 0 || drs.registerCalls != 0 {
				t.Fatalf("destination calls = upload URL %d, DRS registration %d; want none", output.uploadURLCalls, drs.registerCalls)
			}
			if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
				t.Fatalf("target object stat error = %v; want target absent", err)
			}
		})
	}
}

func TestCopyRecordPreservesNilAndEmptyDescriptions(t *testing.T) {
	empty := ""
	for _, tc := range []struct {
		name        string
		description *string
		wantPresent bool
	}{
		{name: "nil", description: nil},
		{name: "empty", description: &empty, wantPresent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drs := copyRecordWithDescription(t, tc.description)
			if drs.registered == nil || len(drs.registered.Candidates) != 1 {
				t.Fatalf("registered candidates = %+v; want one", drs.registered)
			}
			got := drs.registered.Candidates[0].Description
			if (got != nil) != tc.wantPresent || (got != nil && *got != "") {
				t.Fatalf("registered description = %v; want present=%t, empty value", got, tc.wantPresent)
			}
		})
	}
}

func copyRecordWithDescription(t *testing.T, description *string) *copyDRSAPI {
	t.Helper()
	payload := []byte("description copy")
	sourcePath := filepath.Join(t.TempDir(), "source.txt")
	targetPath := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(sourcePath, payload, 0o600); err != nil {
		t.Fatalf("WriteFile source: %v", err)
	}
	sourceAPI := &copyInternalAPI{downloadURLs: map[string]string{"did-description": fileURL(t, sourcePath)}}
	targetAPI := &copyInternalAPI{uploadURL: fileURL(t, targetPath)}
	drs := &copyDRSAPI{}
	logger := logs.NewGen3Logger(slog.New(slog.NewTextHandler(io.Discard, nil)), "", "syfon")
	sourceClient := &copyRecordClient{data: services.NewDataService(sourceAPI, nil, logger, nil)}
	targetDRS := services.NewDRSService(drs)
	targetClient := &copyRecordClient{data: services.NewDataService(targetAPI, nil, logger, targetDRS), drs: targetDRS}
	size := int64(len(payload))
	rec := internalapi.InternalRecord{
		Did:         "did-description",
		Name:        stringPtr("description.txt"),
		Description: description,
		Size:        &size,
	}
	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	if err := copyRecord(context.Background(), cmd, sourceClient, targetClient, rec, "target-bucket", "s3", "s3://target-bucket/org/project", "/organization/org/project/project", 1, 1, t.TempDir()); err != nil {
		t.Fatalf("copyRecord returned error: %v", err)
	}
	return drs
}

func fileURL(t *testing.T, filePath string) string {
	t.Helper()
	parsed := &url.URL{Scheme: "file", Path: filePath}
	return parsed.String()
}
