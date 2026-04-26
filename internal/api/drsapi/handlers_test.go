package drsapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestDRSHandlers(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj": {
				Id:      "test-obj",
				Name:    common.Ptr("test-file"),
				Size:    100,
				SelfUri: "drs://test-obj",
				AccessMethods: &[]drs.AccessMethod{
					{
						AccessId: common.Ptr("s3-access"),
						Type:     drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/key"},
					},
				},
			},
		},
	}
	um := &testutils.MockUrlManager{}
	om := core.NewObjectManager(db, um)
	app := fiber.New()
	RegisterDRSRoutes(app, om)

	t.Run("GetObject_Success", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/test-obj", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var obj drs.DrsObject
		json.NewDecoder(resp.Body).Decode(&obj)
		if obj.Id != "test-obj" {
			t.Errorf("expected test-obj, got %s", obj.Id)
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
	})

	t.Run("GetAccessURL_NotFound", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/test-obj/access/wrong-id", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", resp.StatusCode)
		}
	})

	t.Run("GetBulkAccessURL_Success", func(t *testing.T) {
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
	})

	t.Run("GetServiceInfo", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/service-info", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
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

func TestRegisterObjects(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	um := &testutils.MockUrlManager{}
	om := core.NewObjectManager(db, um)
	app := fiber.New()
	RegisterDRSRoutes(app, om)

	t.Run("Register_Single", func(t *testing.T) {
		size := int64(50)
		authz := map[string][]string{"org1": {"proj1"}}
		cand := drs.DrsObjectCandidate{
			Size: size,
			Checksums: []drs.Checksum{
				{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			},
			AccessMethods: &[]drs.AccessMethod{{
				Type: "s3",
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/org1/proj1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				Authorizations: &authz,
			}},
		}
		body, _ := json.Marshal(cand)
		req := httptest.NewRequest("POST", "/objects/register", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201, got %d. check internal/api/apiutil/error.go for mapping", resp.StatusCode)
		}

		var created drs.N201ObjectsCreated
		if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(created.Objects) != 1 || created.Objects[0].Id == "" {
			t.Errorf("unexpected response: %+v", created)
		}
		if created.Objects[0].AccessMethods == nil || len(*created.Objects[0].AccessMethods) == 0 {
			t.Fatalf("expected access methods in response: %+v", created.Objects[0])
		}
		if (*created.Objects[0].AccessMethods)[0].Authorizations == nil || len(*(*created.Objects[0].AccessMethods)[0].Authorizations) == 0 {
			t.Fatalf("expected authorizations in response: %+v", created.Objects[0].AccessMethods)
		}
	})

	t.Run("Register_Single_AuthExtension", func(t *testing.T) {
		body := []byte(`{
			"size": 64,
			"checksums": [{"type": "sha256", "checksum": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}],
			"auth": {
				"org2": {
					"proj2": ["s3://bucket/path/to/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"]
				}
			}
		}`)
		req := httptest.NewRequest("POST", "/objects/register", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201, got %d", resp.StatusCode)
		}

		var created struct {
			Objects []map[string]any `json:"objects"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(created.Objects) != 1 {
			t.Fatalf("expected one object, got %+v", created)
		}
		auth, ok := created.Objects[0]["auth"].(map[string]any)
		if !ok {
			t.Fatalf("expected auth extension in response: %+v", created.Objects[0])
		}
		org, ok := auth["org2"].(map[string]any)
		if !ok {
			t.Fatalf("expected org2 auth scope in response: %+v", auth)
		}
		paths, ok := org["proj2"].([]any)
		if !ok || len(paths) != 1 || paths[0] != "s3://bucket/path/to/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" {
			t.Fatalf("expected project path in auth extension: %+v", org)
		}
	})

	t.Run("Register_Bulk", func(t *testing.T) {
		size := int64(100)
		bodyObj := struct {
			Candidates []drs.DrsObjectCandidate `json:"candidates"`
		}{
			Candidates: []drs.DrsObjectCandidate{
				{Size: size, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}},
				{Size: size, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}},
			},
		}
		body, _ := json.Marshal(bodyObj)
		req := httptest.NewRequest("POST", "/objects/register", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201, got %d", resp.StatusCode)
		}
	})
}

func TestAdditionalDRSHandlers(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"checksum-obj": {
				Id:   "checksum-obj",
				Size: 200,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"},
				},
			},
		},
	}
	um := &testutils.MockUrlManager{}
	om := core.NewObjectManager(db, um)
	app := fiber.New()
	RegisterDRSRoutes(app, om)

	t.Run("GetObjectsByChecksum", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/objects/checksum/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", nil)
		resp, _ := app.Test(req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var list drs.N200OkDrsObjectsJSONResponse
		json.NewDecoder(resp.Body).Decode(&list)
		if list.Summary.Resolved == nil || *list.Summary.Resolved != 1 {
			t.Errorf("expected 1 resolved object, got %v", list.Summary.Resolved)
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
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("BulkDeleteObjects", func(t *testing.T) {
		db.Objects["bulk-delete-a"] = &drs.DrsObject{Id: "bulk-delete-a", Size: 1}
		db.Objects["bulk-delete-b"] = &drs.DrsObject{Id: "bulk-delete-b", Size: 1}
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
		db.Objects["bulk-update-a"] = &drs.DrsObject{Id: "bulk-update-a", Size: 1}
		db.Objects["bulk-update-b"] = &drs.DrsObject{Id: "bulk-update-b", Size: 1}
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
