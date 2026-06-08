package internaldrs

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/apigen/server/bucketapi"
	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestHandleInternalDeleteProject_RemovesObjectsAndBucketScopes(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"delete-me": {Id: "delete-me", Name: common.Ptr("delete-me")},
			"keep-me":   {Id: "keep-me", Name: common.Ptr("keep-me")},
		},
		ObjectAuthz: map[string]map[string][]string{
			"delete-me": {"org-a": {"proj-a"}},
			"keep-me":   {"org-a": {"proj-b"}},
		},
		BucketScopes: map[string]models.BucketScope{
			"org-a|proj-a": {Organization: "org-a", ProjectID: "proj-a", CredentialID: "bucket-a", Bucket: "bucket-a"},
			"org-a|proj-b": {Organization: "org-a", ProjectID: "proj-b", CredentialID: "bucket-b", Bucket: "bucket-b"},
		},
	}
	req := httptest.NewRequest(http.MethodDelete, "/data/projects/org-a/proj-a", nil)
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", true, map[string]map[string]bool{
		"/programs/org-a/projects/proj-a": {"delete": true, "update": true},
	}))

	rr := doInternalDRSTestRequest(req, core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if _, ok := mockDB.Objects["delete-me"]; ok {
		t.Fatalf("expected scoped object to be deleted")
	}
	if _, ok := mockDB.Objects["keep-me"]; !ok {
		t.Fatalf("expected unrelated object to remain")
	}
	if _, ok := mockDB.BucketScopes["org-a|proj-a"]; ok {
		t.Fatalf("expected scoped bucket mapping to be deleted")
	}
	if _, ok := mockDB.BucketScopes["org-a|proj-b"]; !ok {
		t.Fatalf("expected unrelated bucket mapping to remain")
	}

	var resp projectCleanupResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.DeletedObjects != 1 || resp.DeletedBucketScopes != 1 {
		t.Fatalf("unexpected cleanup counts: %+v", resp)
	}
}

func TestHandleInternalDeleteProject_RequiresGen3Auth(t *testing.T) {
	req := httptest.NewRequest(http.MethodDelete, "/data/projects/org-a/proj-a", nil)
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", false, nil))
	rr := doInternalDRSTestRequest(req, core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalBuckets_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1", Region: "us-east-1"}, "b2": {Bucket: "b2", Region: "us-east-1"}},
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", Name: common.Ptr("obj-1"), AccessMethods: &[]drs.AccessMethod{
				{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://b1/path/obj-1"}},
				{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://b2/path/obj-1"}},
			}},
		},
		ObjectAuthz: map[string]map[string][]string{"obj-1": {"cbds": {"proj1"}}},
	}
	req401 := httptest.NewRequest(http.MethodGet, "/data/buckets", nil)
	req401 = req401.WithContext(dataTestAuthContext(req401.Context(), "gen3", false, nil))
	rr401 := doInternalDRSTestRequest(req401, core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr401.Code)
	}
}

func TestHandleInternalPutDeleteBucket_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]models.S3Credential{}}
	region, accessKey, secretKey, endpoint, provider, path := "us-east-1", "ak", "sk", t.TempDir(), "file", "s3://bucket2/cbds/proj1"
	putBody, _ := json.Marshal(bucketapi.PutBucketRequest{Bucket: "bucket2", Provider: &provider, Region: &region, AccessKey: &accessKey, SecretKey: &secretKey, Endpoint: &endpoint, Organization: "cbds", ProjectId: "proj1", Path: &path})
	putReq401 := httptest.NewRequest(http.MethodPut, "/data/buckets", bytes.NewBuffer(putBody))
	putReq401 = putReq401.WithContext(dataTestAuthContext(putReq401.Context(), "gen3", false, nil))
	putRR401 := doInternalDRSTestRequest(putReq401, core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if putRR401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", putRR401.Code)
	}
}

func TestHandleInternalPutBucket_RejectsInvalidGeneratedPayloads(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]models.S3Credential{}}
	req := httptest.NewRequest(http.MethodPut, "/data/buckets", bytes.NewBufferString(`{"bucket":"b2","organization":"cbds","unexpected":"boom"}`))
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", true, map[string]map[string]bool{bucketControlResource: {"create": true}}))
	rr := doInternalDRSTestRequest(req, core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestRegisterInternalRoutes_Smoke(t *testing.T) {
	app := fiber.New()
	om := core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}, Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}}}, &testutils.MockUrlManager{})
	RegisterInternalRoutes(app, om)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/data/upload/abc?bucket=b1", nil))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d body=%s", resp.StatusCode, string(body))
	}
}

func TestRegisteredRoutesByWorkflow(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", Name: ptr("file"), Checksums: []drs.Checksum{{Type: "sha256", Checksum: "sha-1"}}, AccessMethods: &[]drs.AccessMethod{{
				AccessId: ptr("s3"),
				Type:     drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket-a/key"},
			}}},
		},
		Credentials: map[string]models.S3Credential{"bucket-a": {Bucket: "bucket-a", Provider: "s3"}},
	}
	om := core.NewObjectManager(db, &testutils.MockUrlManager{})
	for _, tc := range []struct {
		name string
		req  *http.Request
	}{
		{name: "index", req: httptest.NewRequest(http.MethodGet, "/", nil)},
		{name: "transfer", req: httptest.NewRequest(http.MethodGet, "/data/download/obj-1", nil)},
		{name: "bucket", req: httptest.NewRequest(http.MethodGet, "/data/buckets", nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := doInternalDRSTestRequest(tc.req, om)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", rr.Code)
			}
		})
	}
}

func TestHandleInternalListBucketScopes_Success(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"bucket-a": {CredentialID: "bucket-a", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]models.BucketScope{
			"org-a|proj-a": {Organization: "org-a", ProjectID: "proj-a", CredentialID: "bucket-a", Bucket: "bucket-a", PathPrefix: "path/to/a"},
			"org-a|proj-b": {Organization: "org-a", ProjectID: "proj-b", CredentialID: "bucket-b", Bucket: "bucket-b"},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/data/buckets/bucket-a/scopes", nil)
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", true, nil))

	rr := doInternalDRSTestRequest(req, core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp []bucketapi.BucketScopeResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(resp) != 1 {
		t.Fatalf("expected 1 scope, got %d", len(resp))
	}
	if resp[0].Organization != "org-a" || resp[0].ProjectId != "proj-a" {
		t.Fatalf("unexpected scope: %+v", resp[0])
	}
	if resp[0].Path == nil || *resp[0].Path != "s3://bucket-a/path/to/a" {
		t.Fatalf("unexpected path: %+v", resp[0].Path)
	}
}

func TestHandleInternalListBucketScopes_RequiresGen3Auth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/data/buckets/bucket-a/scopes", nil)
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", false, nil))
	rr := doInternalDRSTestRequest(req, core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalDeleteBucketScope_RequiresExactPathMatch(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"bucket-a": {CredentialID: "bucket-a", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]models.BucketScope{
			"org-a|":       {Organization: "org-a", ProjectID: "", CredentialID: "bucket-a", Bucket: "bucket-a", PathPrefix: "lab"},
			"org-a|proj-a": {Organization: "org-a", ProjectID: "proj-a", CredentialID: "bucket-a", Bucket: "bucket-a", PathPrefix: "lab/project-a"},
		},
	}
	req := httptest.NewRequest(http.MethodDelete, "/data/buckets/bucket-a/scopes?organization=org-a&path=s3://bucket-a/lab", nil)
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", true, map[string]map[string]bool{
		"/programs/org-a": {"delete": true, "update": true},
	}))

	rr := doInternalDRSTestRequest(req, core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body=%s", rr.Code, rr.Body.String())
	}
	if _, ok := mockDB.BucketScopes["org-a|"]; ok {
		t.Fatalf("expected exact path org-only scope to be deleted")
	}
	if _, ok := mockDB.BucketScopes["org-a|proj-a"]; !ok {
		t.Fatalf("expected project scope to remain")
	}
}
