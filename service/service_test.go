package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
	"github.com/google/uuid"
)

func testAccessURL(url string) *struct {
	Headers *[]string `json:"headers,omitempty"`
	Url     string    `json:"url"`
} {
	return &struct {
		Headers *[]string `json:"headers,omitempty"`
		Url     string    `json:"url"`
	}{Url: url}
}

func testAccessMethod(accessID, url string) drs.AccessMethod {
	return drs.AccessMethod{
		AccessId:  core.Ptr(accessID),
		Type:      drs.AccessMethodTypeS3,
		AccessUrl: testAccessURL(url),
	}
}

func testAccessMethods(methods ...drs.AccessMethod) *[]drs.AccessMethod {
	return &methods
}

func testAccessMethodAuthz(issuers ...string) *struct {
	BearerAuthIssuers   *[]string `json:"bearer_auth_issuers,omitempty"`
	DrsObjectId         *string   `json:"drs_object_id,omitempty"`
	PassportAuthIssuers *[]string `json:"passport_auth_issuers,omitempty"`
	SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
} {
	return &struct {
		BearerAuthIssuers   *[]string `json:"bearer_auth_issuers,omitempty"`
		DrsObjectId         *string   `json:"drs_object_id,omitempty"`
		PassportAuthIssuers *[]string `json:"passport_auth_issuers,omitempty"`
		SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
	}{BearerAuthIssuers: &issuers}
}

func TestGetAccessURL(t *testing.T) {
	// Setup mock DB
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj-id": {
				Id: "test-obj-id",
				AccessMethods: testAccessMethods(testAccessMethod("test-access-id", "s3://bucket/key")),
			},
		},
	}

	// Setup mock UrlManager
	mockUrlManager := &testutils.MockUrlManager{}

	// Create service
	service := NewObjectsAPIService(mockDB, mockUrlManager)

	// Test GetAccessURL
	ctx := context.Background()
	resp, err := service.GetAccessURL(ctx, "test-obj-id", "test-access-id")
	if err != nil {
		t.Fatalf("GetAccessURL failed: %v", err)
	}

	if resp.Code != 200 {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	accessURL, ok := resp.Body.(drs.AccessMethodAccessUrl)
	if !ok {
		t.Fatalf("expected body to be AccessMethodAccessUrl, got %T", resp.Body)
	}

	expectedURL := "s3://bucket/key?signed=true"
	if accessURL.Url != expectedURL {
		t.Errorf("expected URL %s, got %s", expectedURL, accessURL.Url)
	}

	usage, ok := mockDB.Usage["test-obj-id"]
	if !ok {
		t.Fatalf("expected usage metric for object to be recorded")
	}
	if usage.DownloadCount != 1 {
		t.Fatalf("expected download count 1, got %d", usage.DownloadCount)
	}
}

func TestPostAccessURL(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj-id": {
				Id: "test-obj-id",
				AccessMethods: testAccessMethods(testAccessMethod("test-access-id", "s3://bucket/key")),
			},
		},
	}
	mockUrlManager := &testutils.MockUrlManager{}
	service := NewObjectsAPIService(mockDB, mockUrlManager)

	ctx := context.Background()
	resp, err := service.PostAccessURL(ctx, "test-obj-id", "test-access-id", drs.PostAccessUrlRequestObject{})
	if err != nil {
		t.Fatalf("PostAccessURL failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}

func TestRegisterObjects(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	mockUM := &testutils.MockUrlManager{}
	service := NewObjectsAPIService(mockDB, mockUM)

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: core.Ptr("new-obj"),
				Size: 100,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "abc123"},
				},
			},
		},
	}
	resp, err := service.RegisterObjects(context.Background(), req)
	if err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}
	if resp.Code != 201 {
		t.Errorf("expected 201, got %d", resp.Code)
	}
	if _, ok := resp.Body.(drs.RegisterObjects201Response); !ok {
		t.Fatalf("expected RegisterObjects201Response, got %T", resp.Body)
	}
	out := resp.Body.(drs.RegisterObjects201Response)
	if len(out.Objects) != 1 {
		t.Fatalf("expected 1 registered object, got %d", len(out.Objects))
	}
	if _, err := uuid.Parse(out.Objects[0].Id); err != nil {
		t.Fatalf("expected UUID object id, got %q", out.Objects[0].Id)
	}
	if len(out.Objects[0].Checksums) == 0 || out.Objects[0].Checksums[0].Checksum != "abc123" {
		t.Fatalf("expected sha256 checksum to be preserved, got %+v", out.Objects[0].Checksums)
	}
}
func TestGetObject(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj": {Id: "test-obj", Size: 500},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	resp, err := service.GetObject(context.Background(), "test-obj", false)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}

func TestBulkUpdateAccessMethods(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {
				Id: "obj-1",
			},
		},
		ObjectAuthz: map[string][]string{
			"obj-1": {"/data_file"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.BulkAccessMethodUpdateRequest{
		Updates: []struct {
			AccessMethods []drs.AccessMethod `json:"access_methods"`
			ObjectId      string            `json:"object_id"`
		}{
			{
				ObjectId: "obj-1",
				AccessMethods: []drs.AccessMethod{{Type: "s3", AccessUrl: testAccessURL("s3://b/k")}},
			},
		},
	}
	resp, err := service.BulkUpdateAccessMethods(context.Background(), req)
	if err != nil {
		t.Fatalf("BulkUpdateAccessMethods failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}

func TestAddChecksums_SuccessAddsOnlyNewTypes(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {
				Id: "obj-1",
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "abc123"},
				},
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"update": true},
	})

	resp, err := service.AddChecksums(ctx, "obj-1", []drs.Checksum{
		{Type: "sha-256", Checksum: "should-be-ignored"},
		{Type: "md5", Checksum: "md5sum"},
	})
	if err != nil {
		t.Fatalf("AddChecksums failed: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	obj, ok := resp.Body.(drs.DrsObject)
	if !ok {
		t.Fatalf("expected DrsObject response, got %T", resp.Body)
	}

	hasSHA256 := false
	hasMD5 := false
	for _, cs := range obj.Checksums {
		switch cs.Type {
		case "sha256":
			hasSHA256 = true
		case "md5":
			if cs.Checksum == "md5sum" {
				hasMD5 = true
			}
		}
	}
	if !hasSHA256 || !hasMD5 {
		t.Fatalf("expected sha256+md5 checksums, got %+v", obj.Checksums)
	}
}

func TestBulkAddChecksums_Success(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", Checksums: []drs.Checksum{{Type: "sha256", Checksum: "a1"}}},
			"obj-2": {Id: "obj-2", Checksums: []drs.Checksum{{Type: "sha256", Checksum: "b2"}}},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"update": true},
	})

	resp, err := service.BulkAddChecksums(ctx, map[string][]drs.Checksum{
		"obj-1": {{Type: "md5", Checksum: "m1"}},
		"obj-2": {{Type: "crc32c", Checksum: "c2"}},
	})
	if err != nil {
		t.Fatalf("BulkAddChecksums failed: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	out, ok := resp.Body.(drs.BulkUpdateAccessMethods200Response)
	if !ok {
		t.Fatalf("expected BulkUpdateAccessMethods200Response, got %T", resp.Body)
	}
	if len(out.Objects) != 2 {
		t.Fatalf("expected 2 updated objects, got %d", len(out.Objects))
	}
}

func TestBulkAddChecksums_Forbidden(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", Checksums: []drs.Checksum{{Type: "sha256", Checksum: "a1"}}},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"read": true},
	})

	resp, err := service.BulkAddChecksums(ctx, map[string][]drs.Checksum{
		"obj-1": {{Type: "md5", Checksum: "m1"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.Code)
	}
}

func TestRegisterObjects_ForbiddenWithoutCreatePermission(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: core.Ptr("obj"),
				Size: 1,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "deadbeef"},
				},
				AccessMethods: testAccessMethods(drs.AccessMethod{
					Type:      "s3",
					AccessUrl: testAccessURL("s3://b/deadbeef"),
					Authorizations: testAccessMethodAuthz("/programs/p/projects/x"),
				}),
			},
		},
	}

	ctx := context.WithValue(context.Background(), core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{})
	resp, err := service.RegisterObjects(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != 403 {
		t.Fatalf("expected 403, got %d", resp.Code)
	}
}

func TestRegisterObjects_UsesAccessMethodAuthzScope(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: core.Ptr("obj"),
				Size: 1,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "a1b2"},
				},
				AccessMethods: testAccessMethods(drs.AccessMethod{
					Type:      "s3",
					AccessUrl: testAccessURL("s3://b/a1b2"),
					Authorizations: testAccessMethodAuthz("/programs/cbdsTest/projects/git_drs_e2e_test"),
				}),
			},
		},
	}

	ctx := context.WithValue(context.Background(), core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbdsTest/projects/git_drs_e2e_test": {"create": true},
	})
	resp, err := service.RegisterObjects(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.Code)
	}
}

func TestRegisterObjects_FileUploadFallbackAllowed(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: core.Ptr("obj"),
				Size: 1,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "cafebabe"},
				},
			},
		},
	}

	ctx := context.WithValue(context.Background(), core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"file_upload": true},
	})
	resp, err := service.RegisterObjects(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != 201 {
		t.Fatalf("expected 201, got %d", resp.Code)
	}
}

func TestGetBulkObjects_TracksMissingAndDenied(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-ok": {
				Id: "obj-ok",
			},
			"obj-denied": {
				Id: "obj-denied",
			},
		},
		ObjectAuthz: map[string][]string{
			"obj-ok":     {"/programs/a/projects/b"},
			"obj-denied": {"/programs/a/projects/c"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"read": true},
	})

	resp, err := service.GetBulkObjects(ctx, drs.GetBulkObjectsRequestObject{
		Body: &drs.GetBulkObjectsJSONRequestBody{
			BulkObjectIds: []string{"obj-ok", "obj-denied", "obj-missing"},
		},
	}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}

	body, ok := resp.Body.(drs.GetBulkObjects200Response)
	if !ok {
		t.Fatalf("expected GetBulkObjects200Response, got %T", resp.Body)
	}
	if core.IntVal(body.Summary.Requested) != 3 || core.IntVal(body.Summary.Resolved) != 1 || core.IntVal(body.Summary.Unresolved) != 1 {
		t.Fatalf("unexpected summary: %+v", body.Summary)
	}
	if len(*body.UnresolvedDrsObjects) != 2 {
		t.Fatalf("expected 2 unresolved groups, got %d", len(*body.UnresolvedDrsObjects))
	}
}

func TestGetBulkAccessURL_UnresolvedCodes(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"ok": {
				Id: "ok",
				AccessMethods: testAccessMethods(testAccessMethod("s3", "s3://bucket/ok")),
			},
			"denied": {
				Id: "denied",
				AccessMethods: testAccessMethods(testAccessMethod("s3", "s3://bucket/denied")),
			},
		},
		ObjectAuthz: map[string][]string{
			"ok":     {"/programs/a/projects/b"},
			"denied": {"/programs/a/projects/c"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"read": true},
	})

	resp, err := service.GetBulkAccessURL(ctx, drs.BulkObjectAccessId{
		BulkObjectAccessIds: &[]struct {
			BulkAccessIds *[]string `json:"bulk_access_ids,omitempty"`
			BulkObjectId  *string   `json:"bulk_object_id,omitempty"`
		}{
			{BulkObjectId: core.Ptr("ok"), BulkAccessIds: &[]string{"s3"}},
			{BulkObjectId: core.Ptr("denied"), BulkAccessIds: &[]string{"s3"}},
			{BulkObjectId: core.Ptr("missing"), BulkAccessIds: &[]string{"s3"}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}

	body, ok := resp.Body.(drs.GetBulkAccessUrl200Response)
	if !ok {
		t.Fatalf("expected GetBulkAccessUrl200Response, got %T", resp.Body)
	}
	if core.IntVal(body.Summary.Requested) != 3 || core.IntVal(body.Summary.Resolved) != 1 || core.IntVal(body.Summary.Unresolved) != 2 {
		t.Fatalf("unexpected summary: %+v", body.Summary)
	}
	if len(*body.UnresolvedDrsObjects) != 2 {
		t.Fatalf("expected 2 unresolved groups, got %d", len(*body.UnresolvedDrsObjects))
	}
}

func TestBulkUpdateAccessMethods_ForbiddenInGen3NoAuthHeader(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {
				Id: "obj-1",
			},
		},
		ObjectAuthz: map[string][]string{
			"obj-1": {"/programs/a/projects/b"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	resp, err := service.BulkUpdateAccessMethods(ctx, drs.BulkAccessMethodUpdateRequest{
		Updates: []struct {
			AccessMethods []drs.AccessMethod `json:"access_methods"`
			ObjectId      string            `json:"object_id"`
		}{
			{
				ObjectId: "obj-1",
				AccessMethods: []drs.AccessMethod{{Type: "s3", AccessUrl: testAccessURL("s3://b/k")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}
}

func TestDeleteObject_SuccessAndForbidden(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-del": {
				Id: "obj-del",
			},
		},
		ObjectAuthz: map[string][]string{
			"obj-del": {"/programs/a/projects/b"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	ctxForbidden := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctxForbidden = context.WithValue(ctxForbidden, core.AuthHeaderPresentKey, true)
	ctxForbidden = context.WithValue(ctxForbidden, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"read": true},
	})
	resp, err := service.DeleteObject(ctxForbidden, "obj-del", drs.DeleteRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp.Code)
	}

	ctxAllowed := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctxAllowed = context.WithValue(ctxAllowed, core.AuthHeaderPresentKey, true)
	ctxAllowed = context.WithValue(ctxAllowed, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"delete": true},
	})
	resp, err = service.DeleteObject(ctxAllowed, "obj-del", drs.DeleteRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.Code)
	}
}

func TestBulkDeleteObjects_Validation(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	resp, err := service.BulkDeleteObjects(context.Background(), drs.BulkDeleteRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
}

func TestOptionsObjectAndBulkObject(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {
				Id: "obj-1",
			},
			"obj-2": {
				Id: "obj-2",
			},
		},
		ObjectAuthz: map[string][]string{
			"obj-1": {"/programs/a/projects/b"},
			"obj-2": {"/programs/a/projects/c"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	resp, err := service.OptionsObject(context.Background(), "obj-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}

	bulkResp, err := service.OptionsBulkObject(context.Background(), drs.BulkObjectIdNoPassport{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bulkResp.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", bulkResp.Code)
	}
}

func TestService_HelperAndLookupMethods(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-1": {
				Id:      "sha-1",
				Size:    1,
				Version: core.Ptr("1"),
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "sha-1"},
				},
				AccessMethods: testAccessMethods(drs.AccessMethod{
					Type:      "s3",
					AccessUrl: testAccessURL("s3://bucket/sha-1"),
					Authorizations: testAccessMethodAuthz("/programs/a/projects/b"),
				}),
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	resp, err := service.GetObjectsByChecksum(context.Background(), "sha256:sha-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}

	missingResp, err := service.GetObjectsByChecksum(context.Background(), "sha256:missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if missingResp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", missingResp.Code)
	}

	bulkResp, err := service.GetObjectsByChecksums(context.Background(), []string{"sha-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bulkResp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", bulkResp.Code)
	}

	mismatchResp, err := service.GetObjectsByChecksum(context.Background(), "md5:sha-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mismatchResp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for type mismatch, got %d", mismatchResp.Code)
	}

	typedBulkResp, err := service.GetObjectsByChecksums(context.Background(), []string{"sha256:sha-1", "md5:sha-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if typedBulkResp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", typedBulkResp.Code)
	}
	typedBody, ok := typedBulkResp.Body.(map[string][]drs.DrsObject)
	if !ok {
		t.Fatalf("unexpected typed bulk body type: %T", typedBulkResp.Body)
	}
	if len(typedBody["sha256:sha-1"]) != 1 {
		t.Fatalf("expected one sha256 match, got %+v", typedBody["sha256:sha-1"])
	}
	if len(typedBody["md5:sha-1"]) != 0 {
		t.Fatalf("expected no md5 matches, got %+v", typedBody["md5:sha-1"])
	}
}

func TestUpdateObjectAccessMethods_Success(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-upd": {
				Id: "obj-upd",
			},
		},
		ObjectAuthz: map[string][]string{
			"obj-upd": {"/programs/a/projects/b"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"update": true},
	})

	resp, err := service.UpdateObjectAccessMethods(ctx, "obj-upd", drs.AccessMethodUpdateRequest{
		AccessMethods: []drs.AccessMethod{
			{Type: "s3", AccessUrl: testAccessURL("s3://bucket/new")},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
}

func TestErrorResponseForDBError(t *testing.T) {
	resp := errorResponseForDBError(context.Background(), "test.notfound", core.ErrNotFound)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.Code)
	}

	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	resp = errorResponseForDBError(ctx, "test.unauthorized", core.ErrUnauthorized)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}

	resp = errorResponseForDBError(context.Background(), "test.internal", errors.New("boom"))
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.Code)
	}
}

func TestAuthorizationsForObjectFallback(t *testing.T) {
	obj := &drs.DrsObject{
		Id:            "obj",
		AccessMethods: testAccessMethods(drs.AccessMethod{
			Authorizations: testAccessMethodAuthz("/programs/a/projects/b", "/programs/a/projects/b"),
		}),
	}
	auth := authorizationsForObject(&core.InternalObject{DrsObject: *obj})
	if len(*auth.BearerAuthIssuers) != 1 {
		t.Fatalf("expected deduped issuers, got %+v", auth.BearerAuthIssuers)
	}
	if len(uniqueStrings([]string{"a", "a", "", "b"})) != 2 {
		t.Fatalf("expected uniqueStrings dedupe behavior")
	}
}

func TestUniqueStringsCaseInsensitive(t *testing.T) {
	got := uniqueStringsCaseInsensitive([]string{" Alpha ", "alpha", "BETA", "beta", "", "  "})
	if len(got) != 2 {
		t.Fatalf("expected 2 unique case-insensitive values, got %d (%v)", len(got), got)
	}
	if got[0] != " Alpha " || got[1] != "BETA" {
		t.Fatalf("expected first-seen value preservation, got %v", got)
	}
}

func TestPostObjectDelegatesToGetObject(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-post": {Id: "obj-post", Size: 10},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	resp, err := service.PostObject(context.Background(), "obj-post", drs.PostObjectRequestObject{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
}

func TestPostUploadRequest_ReturnsSignedS3Method(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]core.S3Credential{
			"upload-bucket": {Bucket: "upload-bucket", Region: "us-west-2"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.UploadRequest{
		Requests: []drs.UploadRequestObject{
			{
				Name:     "sample.bin",
				Size:     123,
				MimeType: "application/octet-stream",
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "abc123"},
				},
			},
		},
	}
	resp, err := service.PostUploadRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	body, ok := resp.Body.(drs.UploadResponse)
	if !ok {
		t.Fatalf("expected UploadResponse body, got %T", resp.Body)
	}
	if len(body.Responses) != 1 {
		t.Fatalf("expected 1 response object, got %d", len(body.Responses))
	}
	if len(*body.Responses[0].UploadMethods) != 1 {
		t.Fatalf("expected 1 upload method, got %d", len(*body.Responses[0].UploadMethods))
	}
	method := (*body.Responses[0].UploadMethods)[0]
	if method.Type != "s3" {
		t.Fatalf("expected s3 upload method, got %s", method.Type)
	}
	if core.StringVal(method.Region) != "us-west-2" {
		t.Fatalf("expected region us-west-2, got %s", core.StringVal(method.Region))
	}
	if method.AccessUrl.Url == "" {
		t.Fatal("expected signed upload URL")
	}
}

func TestPostUploadRequest_Gen3Unauthorized(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]core.S3Credential{
			"upload-bucket": {Bucket: "upload-bucket", Region: "us-east-1"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.UploadRequest{
		Requests: []drs.UploadRequestObject{
			{
				Name:      "sample.bin",
				Size:      10,
				MimeType:  "application/octet-stream",
				Checksums: []drs.Checksum{{Type: "sha256", Checksum: "deadbeef"}},
			},
		},
	}
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	resp, err := service.PostUploadRequest(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.Code)
	}
}

func TestPostUploadRequest_UsesScopedBucketWhenAuthorized(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]core.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Region: "us-east-1"},
			"bucket-b": {Bucket: "bucket-b", Region: "us-west-2"},
		},
		BucketScopes: map[string]core.BucketScope{
			"cbds|proj2": {
				Organization: "cbds",
				ProjectID:    "proj2",
				Bucket:       "bucket-b",
				PathPrefix:   "cbds/proj2",
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.UploadRequest{
		Requests: []drs.UploadRequestObject{
			{
				Name:      "sample.bin",
				Size:      10,
				MimeType:  "application/octet-stream",
				Checksums: []drs.Checksum{{Type: "sha256", Checksum: "deadbeef"}},
			},
		},
	}
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file":                    {"file_upload": true},
		"/programs/cbds/projects/proj2": {"create": true},
	})

	resp, err := service.PostUploadRequest(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	body, ok := resp.Body.(drs.UploadResponse)
	if !ok {
		t.Fatalf("expected UploadResponse body, got %T", resp.Body)
	}
	if len(body.Responses) != 1 || len(*body.Responses[0].UploadMethods) != 1 {
		t.Fatalf("unexpected upload response shape: %+v", body)
	}
	method := (*body.Responses[0].UploadMethods)[0]
	if core.StringVal(method.Region) != "us-west-2" {
		t.Fatalf("expected scoped bucket region us-west-2, got %s", core.StringVal(method.Region))
	}
	if !strings.Contains(method.AccessUrl.Url, "bucket-b") {
		t.Fatalf("expected signed URL for bucket-b, got %s", method.AccessUrl.Url)
	}
}

func TestPostUploadRequest_FallsBackWhenNoScopedAccess(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]core.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Region: "us-east-1"},
			"bucket-b": {Bucket: "bucket-b", Region: "us-west-2"},
		},
		BucketScopes: map[string]core.BucketScope{
			"cbds|proj2": {
				Organization: "cbds",
				ProjectID:    "proj2",
				Bucket:       "bucket-b",
				PathPrefix:   "cbds/proj2",
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.UploadRequest{
		Requests: []drs.UploadRequestObject{
			{
				Name:      "sample.bin",
				Size:      10,
				MimeType:  "application/octet-stream",
				Checksums: []drs.Checksum{{Type: "sha256", Checksum: "deadbeef"}},
			},
		},
	}
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/data_file": {"file_upload": true},
	})

	resp, err := service.PostUploadRequest(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	body, ok := resp.Body.(drs.UploadResponse)
	if !ok {
		t.Fatalf("expected UploadResponse body, got %T", resp.Body)
	}
	method := (*body.Responses[0].UploadMethods)[0]
	if core.StringVal(method.Region) != "us-east-1" {
		t.Fatalf("expected fallback bucket region us-east-1, got %s", core.StringVal(method.Region))
	}
	if !strings.Contains(method.AccessUrl.Url, "bucket-a") {
		t.Fatalf("expected fallback signed URL for bucket-a, got %s", method.AccessUrl.Url)
	}
}

func TestBulkDeleteObjects_Success(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-a": {Id: "obj-a"},
			"obj-b": {Id: "obj-b"},
		},
		ObjectAuthz: map[string][]string{
			"obj-a": {"/programs/a/projects/b"},
			"obj-b": {"/programs/a/projects/b"},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"delete": true},
	})

	resp, err := service.BulkDeleteObjects(ctx, drs.BulkDeleteRequest{
		BulkObjectIds: []string{"obj-a", "obj-b"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.Code)
	}
}

func TestGetServiceInfo(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	resp, err := service.GetServiceInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
}

func TestGetBulkObjects_TooLarge(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	ids := make([]string, defaultMaxBulkRequestLength+1)
	for i := range ids {
		ids[i] = "obj"
	}
	resp, err := service.GetBulkObjects(context.Background(), drs.GetBulkObjectsRequestObject{
		Body: &drs.GetBulkObjectsJSONRequestBody{BulkObjectIds: ids},
	}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.Code)
	}
}

func TestRegisterObjects_TooLarge(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	candidates := make([]drs.DrsObjectCandidate, defaultMaxRegisterRequestLength+1)
	for i := range candidates {
		candidates[i] = drs.DrsObjectCandidate{
			Name: core.Ptr("obj"),
			Size: 1,
			Checksums: []drs.Checksum{
				{Type: "sha256", Checksum: "deadbeef"},
			},
		}
	}
	resp, err := service.RegisterObjects(context.Background(), drs.RegisterObjectsRequest{Candidates: candidates})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.Code)
	}
}

func TestBulkDeleteObjects_TooLarge(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	ids := make([]string, defaultMaxBulkDeleteLength+1)
	for i := range ids {
		ids[i] = "obj"
	}
	resp, err := service.BulkDeleteObjects(context.Background(), drs.BulkDeleteRequest{BulkObjectIds: ids})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.Code)
	}
}

func TestBulkUpdateAccessMethods_TooLarge(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	updates := make([]struct {
		AccessMethods []drs.AccessMethod `json:"access_methods"`
		ObjectId      string            `json:"object_id"`
	}, defaultMaxBulkAccessMethodUpdateLength+1)
	for i := range updates {
		updates[i] = struct {
			AccessMethods []drs.AccessMethod `json:"access_methods"`
			ObjectId      string            `json:"object_id"`
		}{ObjectId: "obj"}
	}
	resp, err := service.BulkUpdateAccessMethods(context.Background(), drs.BulkAccessMethodUpdateRequest{Updates: updates})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.Code)
	}
}

func TestBulkAddChecksums_TooLarge(t *testing.T) {
	service := NewObjectsAPIService(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	updates := make(map[string][]drs.Checksum, defaultMaxBulkChecksumAdditionLength+1)
	for i := 0; i < defaultMaxBulkChecksumAdditionLength+1; i++ {
		updates[fmt.Sprintf("obj-%d", i)] = []drs.Checksum{{Type: "md5", Checksum: "m"}}
	}
	resp, err := service.BulkAddChecksums(context.Background(), updates)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.Code)
	}
}
