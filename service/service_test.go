package service

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
)

func TestGetAccessURL(t *testing.T) {
	// Setup mock DB
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj-id": {
				Id: "test-obj-id",
				AccessMethods: []drs.AccessMethod{
					{
						AccessId: "test-access-id",
						Type:     "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/key",
						},
					},
				},
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
}

func TestPostAccessURL(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj-id": {
				Id: "test-obj-id",
				AccessMethods: []drs.AccessMethod{
					{
						AccessId: "test-access-id",
						Type:     "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/key",
						},
					},
				},
			},
		},
	}
	mockUrlManager := &testutils.MockUrlManager{}
	service := NewObjectsAPIService(mockDB, mockUrlManager)

	ctx := context.Background()
	resp, err := service.PostAccessURL(ctx, "test-obj-id", "test-access-id", drs.PostAccessUrlRequest{})
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
				Name: "new-obj",
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
				Id:             "obj-1",
				Authorizations: []string{"/data_file"},
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.BulkAccessMethodUpdateRequest{
		Updates: []drs.BulkAccessMethodUpdateRequestUpdatesInner{
			{
				ObjectId: "obj-1",
				AccessMethods: []drs.AccessMethod{
					{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://b/k"}},
				},
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

func TestRegisterObjects_ForbiddenWithoutCreatePermission(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: "obj",
				Size: 1,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "deadbeef"},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type:      "s3",
						AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://b/deadbeef"},
						Authorizations: drs.AccessMethodAuthorizations{
							BearerAuthIssuers: []string{"/programs/p/projects/x"},
						},
					},
				},
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

func TestRegisterObjects_UsesOrganizationProjectScope(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name:         "obj",
				Size:         1,
				Organization: "cbdsTest",
				Project:      "git_drs_e2e_test",
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "a1b2"},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type:      "s3",
						AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://b/a1b2"},
					},
				},
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

func TestRegisterObjects_ProjectRequiresOrganization(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name:    "obj",
				Size:    1,
				Project: "git_drs_e2e_test",
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "b1c2"},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type:      "s3",
						AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://b/b1c2"},
					},
				},
			},
		},
	}
	resp, err := service.RegisterObjects(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.Code)
	}
}

func TestRegisterObjects_FileUploadFallbackAllowed(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: "obj",
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
				Id:             "obj-ok",
				Authorizations: []string{"/programs/a/projects/b"},
			},
			"obj-denied": {
				Id:             "obj-denied",
				Authorizations: []string{"/programs/a/projects/c"},
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"read": true},
	})

	resp, err := service.GetBulkObjects(ctx, drs.GetBulkObjectsRequest{
		BulkObjectIds: []string{"obj-ok", "obj-denied", "obj-missing"},
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
	if body.Summary.Requested != 3 || body.Summary.Resolved != 1 || body.Summary.Unresolved != 1 {
		t.Fatalf("unexpected summary: %+v", body.Summary)
	}
	if len(body.UnresolvedDrsObjects) != 2 {
		t.Fatalf("expected 2 unresolved groups, got %d", len(body.UnresolvedDrsObjects))
	}
}

func TestGetBulkAccessURL_UnresolvedCodes(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"ok": {
				Id: "ok",
				AccessMethods: []drs.AccessMethod{
					{AccessId: "s3", Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/ok"}},
				},
				Authorizations: []string{"/programs/a/projects/b"},
			},
			"denied": {
				Id: "denied",
				AccessMethods: []drs.AccessMethod{
					{AccessId: "s3", Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/denied"}},
				},
				Authorizations: []string{"/programs/a/projects/c"},
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"read": true},
	})

	resp, err := service.GetBulkAccessURL(ctx, drs.BulkObjectAccessId{
		BulkObjectAccessIds: []drs.BulkObjectAccessIdBulkObjectAccessIdsInner{
			{BulkObjectId: "ok", BulkAccessIds: []string{"s3"}},
			{BulkObjectId: "denied", BulkAccessIds: []string{"s3"}},
			{BulkObjectId: "missing", BulkAccessIds: []string{"s3"}},
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
	if body.Summary.Requested != 3 || body.Summary.Resolved != 1 || body.Summary.Unresolved != 2 {
		t.Fatalf("unexpected summary: %+v", body.Summary)
	}
	if len(body.UnresolvedDrsObjects) != 2 {
		t.Fatalf("expected 2 unresolved groups, got %d", len(body.UnresolvedDrsObjects))
	}
}

func TestBulkUpdateAccessMethods_ForbiddenInGen3NoAuthHeader(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {
				Id:             "obj-1",
				Authorizations: []string{"/programs/a/projects/b"},
			},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	resp, err := service.BulkUpdateAccessMethods(ctx, drs.BulkAccessMethodUpdateRequest{
		Updates: []drs.BulkAccessMethodUpdateRequestUpdatesInner{
			{
				ObjectId: "obj-1",
				AccessMethods: []drs.AccessMethod{
					{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://b/k"}},
				},
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
				Id:             "obj-del",
				Authorizations: []string{"/programs/a/projects/b"},
			},
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
				Id:             "obj-1",
				Authorizations: []string{"/programs/a/projects/b"},
			},
			"obj-2": {
				Id:             "obj-2",
				Authorizations: []string{"/programs/a/projects/c"},
			},
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

	ctx := context.WithValue(context.Background(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/a/projects/b": {"read": true},
	})
	bulkResp, err := service.OptionsBulkObject(ctx, drs.BulkObjectIdNoPassport{
		BulkObjectIds: []string{"obj-1", "obj-2", "missing"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bulkResp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", bulkResp.Code)
	}
	out, ok := bulkResp.Body.(drs.OptionsBulkObject200Response)
	if !ok {
		t.Fatalf("expected OptionsBulkObject200Response, got %T", bulkResp.Body)
	}
	if out.Summary.Requested != 3 || out.Summary.Resolved != 1 || out.Summary.Unresolved != 1 {
		t.Fatalf("unexpected summary: %+v", out.Summary)
	}
}

func TestService_HelperAndLookupMethods(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-1": {
				Id:      "sha-1",
				Size:    1,
				Version: "1",
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "sha-1"},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type:      "s3",
						AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/sha-1"},
						Authorizations: drs.AccessMethodAuthorizations{
							BearerAuthIssuers: []string{"/programs/a/projects/b"},
						},
					},
				},
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
}

func TestUpdateObjectAccessMethods_Success(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-upd": {
				Id:             "obj-upd",
				Authorizations: []string{"/programs/a/projects/b"},
			},
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
			{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/new"}},
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
		Id: "obj",
		AccessMethods: []drs.AccessMethod{
			{
				Authorizations: drs.AccessMethodAuthorizations{
					BearerAuthIssuers: []string{"/programs/a/projects/b", "/programs/a/projects/b"},
				},
			},
		},
	}
	auth := authorizationsForObject(&core.InternalObject{DrsObject: *obj})
	if len(auth.BearerAuthIssuers) != 1 {
		t.Fatalf("expected deduped issuers, got %+v", auth.BearerAuthIssuers)
	}
	if len(uniqueStrings([]string{"a", "a", "", "b"})) != 2 {
		t.Fatalf("expected uniqueStrings dedupe behavior")
	}
}

func TestPostObjectDelegatesToGetObject(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-post": {Id: "obj-post", Size: 10},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})
	resp, err := service.PostObject(context.Background(), "obj-post", drs.PostObjectRequest{})
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
	if len(body.Responses[0].UploadMethods) != 1 {
		t.Fatalf("expected 1 upload method, got %d", len(body.Responses[0].UploadMethods))
	}
	method := body.Responses[0].UploadMethods[0]
	if method.Type != "s3" {
		t.Fatalf("expected s3 upload method, got %s", method.Type)
	}
	if method.Region != "us-west-2" {
		t.Fatalf("expected region us-west-2, got %s", method.Region)
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

func TestBulkDeleteObjects_Success(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-a": {Id: "obj-a", Authorizations: []string{"/programs/a/projects/b"}},
			"obj-b": {Id: "obj-b", Authorizations: []string{"/programs/a/projects/b"}},
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
