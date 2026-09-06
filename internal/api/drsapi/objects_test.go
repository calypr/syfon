package drsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	httpdrs "github.com/calypr/syfon/internal/httpapi/drs"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
)

type captureStorageAccess struct {
	lastOptions storage.AccessOptions
	lastURL     string
	lastAccess  string
}

func testServiceInfo() drs.Service {
	description := "Calypr test DRS server"
	environment := "test"
	createdAt := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	updatedAt := time.Date(2024, time.January, 3, 4, 5, 6, 0, time.UTC)
	return drs.Service{
		Id:          "drs-service-test",
		Name:        "Calypr Test DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: &description,
		CreatedAt:   &createdAt,
		UpdatedAt:   &updatedAt,
		Environment: &environment,
		Version:     "1.0.0",
	}
}

func (m *captureStorageAccess) Access(_ context.Context, request storage.AccessRequest) (storage.Access, error) {
	m.lastOptions = request.Options
	m.lastURL = request.Target.Location
	m.lastAccess = request.Target.AccessID
	suffix := "?signed=true"
	if request.Options.Method == http.MethodPut || request.Options.Method == http.MethodPost {
		suffix += "&upload=true"
	}
	return storage.Access{Location: request.Target.Location + suffix}, nil
}

func TestDRSHandlers(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{
			"test-obj": {
				Id:      "test-obj",
				Name:    common.Ptr("test-file"),
				Size:    100,
				SelfUri: "drs://test-obj",
				Checksums: []objects.Checksum{
					{Type: "sha256", Checksum: "sha-1"},
				},
				AccessMethods: &[]objects.AccessMethod{
					{
						AccessId:  common.Ptr("s3-access"),
						Type:      "s3",
						AccessUrl: &objects.AccessURL{Url: "s3://bucket/key"},
					},
				},
				Properties: map[string]json.RawMessage{
					"large": json.RawMessage(`9007199254740993`),
				},
			},
		},
		ObjectAuthz: map[string]map[string][]string{
			"test-obj": {"calypr": {"proj-a"}},
		},
	}
	storageAccess := &captureStorageAccess{}
	om := testObjectManager(db, core.StoragePorts{Access: storageAccess})
	app := fiber.New()
	RegisterDRSRoutes(app, om.objectService, om.transferService, testServiceInfo())

	t.Run("GetObject_Success", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/test-obj", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var obj map[string]json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		for key, want := range map[string]string{"id": `"test-obj"`, "did": `"test-obj"`, "large": `9007199254740993`} {
			if got := string(obj[key]); got != want {
				t.Errorf("%s = %s, want %s", key, got, want)
			}
		}
	})

	t.Run("GetObject_NotFound", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/unknown", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", resp.StatusCode)
		}
	})

	t.Run("GetAccessURL_Success", func(t *testing.T) {
		db.TransferEvents = nil
		storageAccess.lastURL = ""
		storageAccess.lastAccess = ""
		req := httptest.NewRequest("GET", "/objects/test-obj/access/s3-access", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var access drs.AccessURL
		json.NewDecoder(resp.Body).Decode(&access)
		if access.Url == "" {
			t.Error("expected signed URL, got empty")
		}
		if got, want := storageAccess.lastOptions.DownloadFilename, "test-file"; got != want {
			t.Fatalf("unexpected download filename override: got %q want %q", got, want)
		}
		if got, want := storageAccess.lastURL, "s3://bucket/key"; got != want {
			t.Fatalf("unexpected signed storage URL: got %q want %q", got, want)
		}
		if len(db.TransferEvents) != 1 {
			t.Fatalf("expected one access-issued event, got %+v", db.TransferEvents)
		}
		ev := db.TransferEvents[0]
		if ev.EventType != "access_issued" || ev.ObjectID != "test-obj" || ev.SHA256 != "sha-1" || ev.Organization != "calypr" || ev.Project != "proj-a" || ev.Provider != "s3" || ev.Bucket != "bucket" {
			t.Fatalf("unexpected access-issued event: %+v", ev)
		}
		if ev.AccessGrantID == "" || ev.AccessGrantID == ev.EventID {
			t.Fatalf("expected stable grant id distinct from audit event id: %+v", ev)
		}
	})

	t.Run("GetAccessURL_MapsLegacyReplica", func(t *testing.T) {
		db := &testutils.MockDatabase{
			Objects: map[string]*objects.Record{
				"scoped-obj": {
					Id:               "scoped-obj",
					Name:             common.Ptr("slide.ome.tiff"),
					Size:             100,
					SelfUri:          "drs://scoped-obj",
					ControlledAccess: &[]string{"/organization/HTAN_INT/project/BForePC"},
					AccessMethods: &[]objects.AccessMethod{{
						AccessId:  common.Ptr("s3"),
						Type:      "s3",
						AccessUrl: &objects.AccessURL{Url: "s3://bforepc-prod/OHSU/slide.ome.tiff"},
					}},
				},
			},
			BucketScopes: map[string]buckets.Scope{
				"HTAN_INT|BForePC": {
					Organization: "HTAN_INT",
					ProjectID:    "BForePC",
					Bucket:       "bforepc",
					PathPrefix:   "bforepc-prod",
				},
			},
		}
		storageAccess := &captureStorageAccess{}
		om := testObjectManager(db, core.StoragePorts{Access: storageAccess})
		app := fiber.New()
		RegisterDRSRoutes(app, om.objectService, om.transferService, testServiceInfo())

		req := httptest.NewRequest("GET", "/objects/scoped-obj/access/s3", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		wantURL := "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff"
		if got := storageAccess.lastURL; got != wantURL {
			t.Fatalf("expected stored replica URL %q, got %q", wantURL, got)
		}
		if got := storageAccess.lastAccess; got != "bforepc" {
			t.Fatalf("expected signer credential bucket %q, got %q", "bforepc", got)
		}
		var access drs.AccessURL
		if err := json.NewDecoder(resp.Body).Decode(&access); err != nil {
			t.Fatalf("failed to decode access URL: %v", err)
		}
		if got, want := access.Url, wantURL+"?signed=true"; got != want {
			t.Fatalf("unexpected signed access URL: got %q want %q", got, want)
		}
	})

	t.Run("GetAccessURL_NotFound", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/test-obj/access/wrong-id", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", resp.StatusCode)
		}
	})

	t.Run("GetBulkAccessURL_Success", func(t *testing.T) {
		db.TransferEvents = nil
		storageAccess.lastURL = ""
		storageAccess.lastAccess = ""
		bodyObj := drs.BulkObjectAccessId{
			BulkObjectAccessIds: &[]struct {
				BulkAccessIds *[]string `json:"bulk_access_ids,omitempty"`
				BulkObjectId  *string   `json:"bulk_object_id,omitempty"`
			}{{
				BulkObjectId:  common.Ptr("test-obj"),
				BulkAccessIds: &[]string{"s3-access"},
			}},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/objects/access", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		var access drs.N200OkAccesses
		if err := json.NewDecoder(resp.Body).Decode(&access); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if access.Summary == nil || access.Summary.Resolved == nil || *access.Summary.Resolved != 1 {
			t.Fatalf("expected one resolved access URL, got %+v", access.Summary)
		}
		if access.ResolvedDrsObjectAccessUrls == nil || len(*access.ResolvedDrsObjectAccessUrls) != 1 || (*access.ResolvedDrsObjectAccessUrls)[0].Url == "" {
			t.Fatalf("expected signed bulk access URL, got %+v", access.ResolvedDrsObjectAccessUrls)
		}
		if got, want := storageAccess.lastOptions.DownloadFilename, "test-file"; got != want {
			t.Fatalf("unexpected bulk download filename override: got %q want %q", got, want)
		}
		if got, want := storageAccess.lastURL, "s3://bucket/key"; got != want {
			t.Fatalf("unexpected bulk signed storage URL: got %q want %q", got, want)
		}
		if len(db.TransferEvents) != 1 {
			t.Fatalf("expected one bulk access-issued event, got %+v", db.TransferEvents)
		}
		ev := db.TransferEvents[0]
		if ev.EventType != "access_issued" || ev.AccessID != "s3-access" || ev.ObjectID != "test-obj" {
			t.Fatalf("unexpected bulk access-issued event: %+v", ev)
		}
	})

	t.Run("GetServiceInfo", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/service-info", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var got drs.Service
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatalf("decode service info response: %v", err)
		}
		want := testServiceInfo()
		if got.Id != want.Id || got.Name != want.Name || got.Type != want.Type || got.Version != want.Version {
			t.Fatalf("unexpected service info identity: got %+v want %+v", got, want)
		}
		if got.Description == nil || want.Description == nil || *got.Description != *want.Description {
			t.Fatalf("unexpected service description: got %v want %v", got.Description, want.Description)
		}
		if got.Environment == nil || want.Environment == nil || *got.Environment != *want.Environment {
			t.Fatalf("unexpected service environment: got %v want %v", got.Environment, want.Environment)
		}
		if got.CreatedAt == nil || got.UpdatedAt == nil || want.CreatedAt == nil || want.UpdatedAt == nil {
			t.Fatalf("expected service timestamps: %+v", got)
		}
		if !got.CreatedAt.Equal(*want.CreatedAt) || !got.UpdatedAt.Equal(*want.UpdatedAt) {
			t.Fatalf("unexpected service timestamps: got %v/%v want %v/%v", got.CreatedAt, got.UpdatedAt, want.CreatedAt, want.UpdatedAt)
		}
	})

	t.Run("UpdateObjectAccessMethods_Success", func(t *testing.T) {
		bodyObj := drs.AccessMethodUpdateRequest{
			AccessMethods: []drs.AccessMethod{{
				AccessId: common.Ptr("s3"),
				Type:     drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/new-key"},
			}},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/objects/test-obj/access-methods", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		updated := db.Objects["test-obj"]
		if updated.AccessMethods == nil || len(*updated.AccessMethods) != 1 || (*updated.AccessMethods)[0].AccessUrl.Url != "s3://bucket/new-key" {
			t.Fatalf("expected updated access method, got %+v", updated.AccessMethods)
		}
	})

	t.Run("DeleteObject_Success", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/objects/test-obj", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("expected 204, got %d", resp.StatusCode)
		}
		if _, ok := db.Objects["test-obj"]; ok {
			t.Error("expected object to be deleted from mock DB")
		}
	})
}

func TestAdditionalDRSHandlers(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{
			"checksum-obj": {
				Id:   "checksum-obj",
				Size: 200,
				Checksums: []objects.Checksum{
					{Type: "sha256", Checksum: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"},
				},
			},
		},
	}
	om := testObjectManager(db, core.StoragePorts{})
	app := fiber.New()
	RegisterDRSRoutes(app, om.objectService, om.transferService, testServiceInfo())

	t.Run("GetObjectsByChecksum", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/checksum/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var list struct {
			Resolved []map[string]json.RawMessage `json:"resolved_drs_object"`
			Summary  drs.Summary                  `json:"summary"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if list.Summary.Resolved == nil || *list.Summary.Resolved != 1 || len(list.Resolved) != 1 {
			t.Fatalf("expected one resolved object, got %+v", list)
		}
		if string(list.Resolved[0]["id"]) != `"checksum-obj"` || string(list.Resolved[0]["did"]) != `"checksum-obj"` {
			t.Fatalf("compatibility IDs missing from checksum response: %+v", list.Resolved[0])
		}
	})

	t.Run("GetBulkObjects", func(t *testing.T) {
		bodyObj := struct {
			BulkObjectIds []string `json:"bulk_object_ids"`
		}{
			BulkObjectIds: []string{"checksum-obj", "unknown"},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/objects", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var list struct {
			Resolved []map[string]json.RawMessage `json:"resolved_drs_object"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if len(list.Resolved) != 1 || string(list.Resolved[0]["id"]) != `"checksum-obj"` || string(list.Resolved[0]["did"]) != `"checksum-obj"` {
			t.Fatalf("compatibility IDs missing from bulk response: %+v", list.Resolved)
		}
	})

	t.Run("UploadRequest", func(t *testing.T) {
		bodyObj := drs.UploadRequest{
			Requests: []drs.UploadRequestObject{
				{
					Name: "new-upload",
					Size: 300,
					Checksums: []drs.Checksum{
						{Type: "sha256", Checksum: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
					},
				},
			},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/upload-request", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})

	t.Run("BulkDeleteObjects", func(t *testing.T) {
		db.Objects["bulk-delete-a"] = &objects.Record{Id: "bulk-delete-a", Size: 1}
		db.Objects["bulk-delete-b"] = &objects.Record{Id: "bulk-delete-b", Size: 1}
		bodyObj := drs.BulkDeleteRequest{
			BulkObjectIds: []string{"bulk-delete-a", "bulk-delete-b"},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/objects/delete", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected 204, got %d", resp.StatusCode)
		}
		if _, ok := db.Objects["bulk-delete-a"]; ok {
			t.Fatal("expected bulk-delete-a to be deleted")
		}
		if _, ok := db.Objects["bulk-delete-b"]; ok {
			t.Fatal("expected bulk-delete-b to be deleted")
		}
	})

	t.Run("BulkUpdateAccessMethods", func(t *testing.T) {
		db.Objects["bulk-update-a"] = &objects.Record{Id: "bulk-update-a", Size: 1}
		db.Objects["bulk-update-b"] = &objects.Record{Id: "bulk-update-b", Size: 1}
		bodyObj := drs.BulkAccessMethodUpdateRequest{
			Updates: []struct {
				AccessMethods []drs.AccessMethod `json:"access_methods"`
				ObjectId      string             `json:"object_id"`
			}{
				{
					ObjectId: "bulk-update-a",
					AccessMethods: []drs.AccessMethod{{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/a"},
					}},
				},
				{
					ObjectId: "bulk-update-b",
					AccessMethods: []drs.AccessMethod{{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/b"},
					}},
				},
			},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/objects/access-methods", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		for id, want := range map[string]string{"bulk-update-a": "s3://bucket/a", "bulk-update-b": "s3://bucket/b"} {
			got := db.Objects[id]
			if got.AccessMethods == nil || len(*got.AccessMethods) != 1 || (*got.AccessMethods)[0].AccessUrl.Url != want {
				t.Fatalf("expected %s access method %s, got %+v", id, want, got.AccessMethods)
			}
		}
	})
}

func TestChecksumRouteRegression_WithRealCoreAndDB(t *testing.T) {
	database := testutils.NewInMemoryDB()
	om := testObjectManager(database, core.StoragePorts{})
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	controlled := []string{"/organization/testorg/project/testproj"}
	_, err := om.RegisterBulk(context.Background(), []objects.Candidate{httpdrs.FromGeneratedCandidate(drs.DrsObjectCandidate{
		Aliases:          common.Ptr([]string{"id:checksum-regression-obj"}),
		ControlledAccess: &controlled,
		Checksums: []drs.Checksum{{
			Type:     "sha256",
			Checksum: checksum,
		}},
		AccessMethods: &[]drs.AccessMethod{{
			Type: "s3",
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: "s3://bucket/checksum-regression-obj"},
		}},
		Size: 123,
	})})
	if err != nil {
		t.Fatalf("seed register bulk failed: %v", err)
	}

	app := fiber.New()
	RegisterDRSRoutes(app, om.objectService, om.transferService, testServiceInfo())

	req := httptest.NewRequest("GET", "/objects/checksum/"+checksum, nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body drs.N200OkDrsObjectsJSONResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Summary.Resolved == nil || *body.Summary.Resolved != 1 {
		t.Fatalf("expected one resolved object, got %+v", body.Summary)
	}
	if body.ResolvedDrsObject == nil || len(*body.ResolvedDrsObject) != 1 || (*body.ResolvedDrsObject)[0].Id != "checksum-regression-obj" {
		t.Fatalf("unexpected resolved objects: %+v", body.ResolvedDrsObject)
	}
}
