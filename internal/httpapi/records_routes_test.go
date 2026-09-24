package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/sqlite"
	"github.com/gofiber/fiber/v3"
)

func TestRecordBoundary(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	RegisterRoutes(app, Dependencies{Objects: objects.NewService(database)}, Options{Internal: true, GA4GH: true})
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tests := []struct {
		name, method, path, body string
		status                   int
		shape                    string
		ids                      []string
		fields                   map[string]string
		absent                   []string
		message                  string
		raw                      string
	}{
		{name: "single create defaults and permissive fields", method: "POST", path: "/index", body: `{"did":"single","name":"/ignored/file.txt","organization":"org","project":"p1","unknown":true}`, status: 201, shape: "object", ids: []string{"single"}, fields: map[string]string{"size": "0", "version": `"1"`, "name_aliases": "[]", "controlled_access": `["/organization/org/project/p1"]`}, absent: []string{"name"}},
		{name: "bulk create order", method: "POST", path: "/index/bulk", body: `{"records":[{"did":"bulk-a","organization":"org","project":"p1","unknown":true},{"did":"bulk-b","organization":"org","project":"p1"}],"unknown":true}`, status: 201, shape: "list", ids: []string{"bulk-a", "bulk-b"}, fields: map[string]string{"size": "0", "version": `"1"`}},
		{name: "single create validation label", method: "POST", path: "/index", body: `{"did":" "}`, status: 400, message: "record invalid"},
		{name: "bulk create validation index", method: "POST", path: "/index/bulk", body: `{"records":[{"did":" "}]}`, status: 400, message: "record[0] invalid"},
		{name: "GET legacy omissions", method: "GET", path: "/index/single", status: 200, shape: "object", ids: []string{"single"}, fields: map[string]string{"id": `"single"`}, absent: []string{"size", "version", "name_aliases"}},
		{name: "list zero and empty values", method: "GET", path: "/index?organization=org&project=p1&limit=1", status: 200, shape: "list", ids: []string{"bulk-a"}, fields: map[string]string{"size": "0", "version": `"1"`, "name_aliases": "[]"}, absent: []string{"id"}},
		{name: "bulk documents array input", method: "POST", path: "/index/bulk/documents", body: `["single"]`, status: 200, shape: "array", ids: []string{"single"}, fields: map[string]string{"size": "0", "version": `"1"`, "name_aliases": "[]"}},
		{name: "bulk documents object input", method: "POST", path: "/index/bulk/documents", body: `{"ids":["single"]}`, status: 200, shape: "array", ids: []string{"single"}},
		{name: "single input at bulk route", method: "POST", path: "/index/bulk", body: `{"did":"single-at-bulk"}`, status: 201, shape: "list", ids: []string{"single-at-bulk"}},
		{name: "bulk hash delete accepts object IDs", method: "POST", path: "/index/bulk/delete", body: `{"hashes":["single-at-bulk"]}`, status: 200, raw: `{"deleted":1}`},
		{name: "bulk input at single route returns first record", method: "POST", path: "/index", body: `{"records":[{"did":"bulk-at-single-a"},{"did":"bulk-at-single-b"}]}`, status: 201, shape: "object", ids: []string{"bulk-at-single-a"}},
		{name: "query fixtures", method: "POST", path: "/index/bulk", body: `{"records":[{"did":"url-a","organization":"org","project":"p1","access_methods":[{"type":"s3","access_url":{"url":"s3://bucket/shared"}}]},{"did":"url-b","organization":"org","project":"p1","access_methods":[{"type":"s3","access_url":{"url":"s3://bucket/shared"}}]},{"did":"other-scope","organization":"other","project":"p2"},{"did":"hash-record","organization":"org","project":"p1","hashes":{"sha256":"` + sha + `"}}]}`, status: 201, shape: "list", ids: []string{"url-a", "url-b", "other-scope", "hash-record"}},
		{name: "checksum duplicate returns durable ID", method: "POST", path: "/index", body: `{"did":"hash-alias","organization":"org","project":"p1","hashes":{"sha256":"` + sha + `"}}`, status: 201, shape: "object", ids: []string{"hash-record"}},
		{name: "URL page", method: "GET", path: "/index?url=s3%3A%2F%2Fbucket%2Fshared&limit=1&page=0", status: 200, shape: "list", ids: []string{"url-a"}},
		{name: "URL cursor overrides page", method: "GET", path: "/index?url=s3%3A%2F%2Fbucket%2Fshared&limit=1&start=url-a&page=99", status: 200, shape: "list", ids: []string{"url-b"}},
		{name: "scoped checksum", method: "GET", path: "/index?organization=org&project=p1&hash=sha256:" + sha + "&limit=1", status: 200, shape: "list", ids: []string{"hash-record"}},
		{name: "scope excludes other projects", method: "GET", path: "/index?organization=org&project=p1", status: 200, shape: "list", ids: []string{"bulk-a", "bulk-b", "hash-record", "single", "url-a", "url-b"}},
		{name: "malformed integer precedes scope validation", method: "GET", path: "/index?limit=bad&project=project-without-org", status: 400, message: "limit"},
		{name: "project requires organization", method: "GET", path: "/index?project=project-without-org", status: 400, message: "organization is required"},
		{name: "update fixture", method: "POST", path: "/index", body: `{"did":"immutable-size","size":7,"organization":"org","project":"p1"}`, status: 201, shape: "object", ids: []string{"immutable-size"}},
		{name: "size is immutable", method: "PUT", path: "/index/immutable-size", body: `{"size":8}`, status: 409, message: "immutable"},
		{name: "explicit zero size is not omission", method: "PUT", path: "/index/immutable-size", body: `{"size":0}`, status: 409, message: "immutable"},
		{name: "omitted size preserves stored size", method: "PUT", path: "/index/immutable-size", body: `{"description":"changed"}`, status: 200, shape: "object", ids: []string{"immutable-size"}, fields: map[string]string{"size": "7", "description": `"changed"`}},
		{name: "update rejects unknown fields", method: "PUT", path: "/index/immutable-size", body: `{"size":7,"unknown_update_field":true}`, status: 400, message: "unknown_update_field"},
	}
	for _, test := range tests {
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		if test.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := app.Test(request)
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		payload, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != test.status {
			t.Fatalf("%s: status %d, want %d: %s", test.name, response.StatusCode, test.status, payload)
		}
		if test.message != "" {
			var failure struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(payload, &failure); err != nil || !strings.Contains(failure.Message, test.message) {
				t.Fatalf("%s: unexpected error: %s", test.name, payload)
			}
			continue
		}
		if test.raw != "" {
			if string(payload) != test.raw {
				t.Fatalf("%s: response %s, want %s", test.name, payload, test.raw)
			}
			continue
		}
		var records []map[string]json.RawMessage
		switch test.shape {
		case "object":
			var record map[string]json.RawMessage
			err = json.Unmarshal(payload, &record)
			records = []map[string]json.RawMessage{record}
		case "array":
			err = json.Unmarshal(payload, &records)
		case "list":
			var body struct {
				Records []map[string]json.RawMessage `json:"records"`
			}
			err = json.Unmarshal(payload, &body)
			records = body.Records
		}
		if err != nil {
			t.Fatalf("%s: invalid %s response: %s", test.name, test.shape, payload)
		}
		ids := make([]string, len(records))
		for i, record := range records {
			if err := json.Unmarshal(record["did"], &ids[i]); err != nil {
				t.Fatalf("%s: missing record ID: %s", test.name, payload)
			}
			for key, want := range test.fields {
				if string(record[key]) != want {
					t.Fatalf("%s: %s=%s, want %s", test.name, key, record[key], want)
				}
			}
			for _, key := range test.absent {
				if _, found := record[key]; found {
					t.Fatalf("%s: unexpected %s in %s", test.name, key, payload)
				}
			}
		}
		if !reflect.DeepEqual(ids, test.ids) {
			t.Fatalf("%s: IDs %v, want %v", test.name, ids, test.ids)
		}
	}
}

func TestInternalBulkHashesResponseUsesResultsMap(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	RegisterRoutes(app, Dependencies{Objects: objects.NewService(database)}, Options{Internal: true, GA4GH: true})
	hash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	createRequest := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(`{"did":"bulk-hash-record","hashes":{"sha256":"`+hash+`"}}`))
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse, err := app.Test(createRequest)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	if createResponse.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResponse.Body)
		createResponse.Body.Close()
		t.Fatalf("create status = %d, want %d: %s", createResponse.StatusCode, http.StatusCreated, body)
	}
	createResponse.Body.Close()

	request := httptest.NewRequest(http.MethodPost, "/index/bulk/hashes", strings.NewReader(`{"hashes":["sha256:`+hash+`","missing-hash"]}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("bulk hashes request failed: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("bulk hashes status = %d, want %d: %s", response.StatusCode, http.StatusOK, body)
	}

	var payload map[string]map[string][]internalapi.InternalRecord
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode bulk hashes response: %v", err)
	}
	if _, found := payload["records"]; found {
		t.Fatalf("bulk hashes response unexpectedly used records: %s", body)
	}
	if _, found := payload["Results"]; found {
		t.Fatalf("bulk hashes response unexpectedly used uppercase Results: %s", body)
	}
	results, found := payload["results"]
	if !found {
		t.Fatalf("bulk hashes response missing lowercase results: %s", body)
	}
	if len(results) != 2 {
		t.Fatalf("results keys = %d, want 2: %s", len(results), body)
	}
	matched, found := results["sha256:"+hash]
	if !found || len(matched) != 1 {
		t.Fatalf("matched records = %+v, want one record: %s", matched, body)
	}
	if matched[0].Did != "bulk-hash-record" {
		t.Fatalf("matched did = %q, want %q", matched[0].Did, "bulk-hash-record")
	}
	if matched[0].Hashes == nil || (*matched[0].Hashes)["sha256"] != hash {
		t.Fatalf("matched hashes = %+v, want sha256=%q", matched[0].Hashes, hash)
	}
	missing, found := results["missing-hash"]
	if !found || len(missing) != 0 {
		t.Fatalf("missing hash records = %+v, want empty array", missing)
	}

	parsed, err := internalapi.ParseInternalBulkHashesResp(&http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	})
	if err != nil {
		t.Fatalf("parse generated bulk hashes response: %v", err)
	}
	if parsed.JSON200 == nil || len(parsed.JSON200.Results["sha256:"+hash]) != 1 {
		t.Fatalf("generated response did not decode results: %+v", parsed.JSON200)
	}
}

func TestDeleteByQueryHonorsChecksumFilter(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	RegisterRoutes(app, Dependencies{Objects: objects.NewService(database)}, Options{Internal: true})
	for _, seed := range []struct{ id, hash string }{
		{"keep", strings.Repeat("a", 64)},
		{"remove", strings.Repeat("b", 64)},
	} {
		body := `{"did":"` + seed.id + `","organization":"org","project":"project","hashes":{"sha256":"` + seed.hash + `"}}`
		request := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response, err := app.Test(request)
		if err != nil || response.StatusCode != http.StatusCreated {
			t.Fatalf("create %s: response=%v err=%v", seed.id, response, err)
		}
		_ = response.Body.Close()
	}

	request := httptest.NewRequest(http.MethodDelete, "/index?organization=org&project=project&hash_type=sha256&hash="+strings.Repeat("b", 64), nil)
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", response.StatusCode)
	}
	var deleted internalapi.DeleteByQueryResponse
	if err := json.NewDecoder(response.Body).Decode(&deleted); err != nil {
		t.Fatal(err)
	}
	if deleted.Deleted == nil || *deleted.Deleted != 1 {
		t.Fatalf("deleted = %v, want 1", deleted.Deleted)
	}

	request = httptest.NewRequest(http.MethodGet, "/index?organization=org&project=project", nil)
	response, err = app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var listed internalapi.ListRecordsResponse
	if err := json.NewDecoder(response.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	if listed.Records == nil || len(*listed.Records) != 1 || (*listed.Records)[0].Did != "keep" {
		t.Fatalf("remaining records = %+v, want keep", listed.Records)
	}
}

func TestInternalBulkSHA256ValidityGeneratedClient(t *testing.T) {
	database, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	RegisterRoutes(app, Dependencies{Objects: objects.NewService(database)}, Options{Internal: true})
	sha := strings.Repeat("a", 64)
	missing := strings.Repeat("b", 64)
	createRequest := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(`{"did":"bulk-validity-record","organization":"org","project":"project","hashes":{"sha256":"`+sha+`"}}`))
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse, err := app.Test(createRequest)
	if err != nil {
		t.Fatalf("create request failed: %v", err)
	}
	if createResponse.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResponse.Body)
		createResponse.Body.Close()
		t.Fatalf("create status = %d, want %d: %s", createResponse.StatusCode, http.StatusCreated, body)
	}
	createResponse.Body.Close()

	client, err := internalapi.NewClientWithResponses("http://syfon.test", internalapi.WithHTTPClient(multipartTestClient{app}))
	if err != nil {
		t.Fatal(err)
	}
	values := []string{sha, missing}
	want := map[string]bool{sha: true, missing: false}
	requests := []struct {
		name string
		body internalapi.BulkSHA256ValidityRequest
	}{
		{name: "hashes alias", body: internalapi.BulkSHA256ValidityRequest{Hashes: &values}},
		{name: "sha256 field", body: internalapi.BulkSHA256ValidityRequest{Sha256: &values}},
		{name: "identical fields", body: internalapi.BulkSHA256ValidityRequest{Hashes: &values, Sha256: &values}},
	}
	for _, test := range requests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.InternalBulkSHA256ValidityWithResponse(context.Background(), test.body)
			if err != nil {
				t.Fatalf("validity request failed: %v", err)
			}
			if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
				t.Fatalf("validity status = %d, want %d: %s", response.StatusCode(), http.StatusOK, response.Body)
			}
			if !reflect.DeepEqual(*response.JSON200, want) {
				t.Fatalf("validity map = %v, want %v", *response.JSON200, want)
			}
		})
	}

	emptyValues := []string{}
	whitespaceValues := []string{" ", "\t\n"}
	differentLengthValues := []string{missing}
	conflictingValues := []string{missing, sha}
	invalidRequests := []struct {
		name string
		body internalapi.BulkSHA256ValidityRequest
	}{
		{name: "empty request"},
		{name: "empty hashes list", body: internalapi.BulkSHA256ValidityRequest{Hashes: &emptyValues}},
		{name: "whitespace-only values", body: internalapi.BulkSHA256ValidityRequest{Hashes: &whitespaceValues}},
		{name: "conflicting fields with different lengths", body: internalapi.BulkSHA256ValidityRequest{Hashes: &values, Sha256: &differentLengthValues}},
		{name: "conflicting fields with different values", body: internalapi.BulkSHA256ValidityRequest{Hashes: &values, Sha256: &conflictingValues}},
	}
	for _, test := range invalidRequests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.InternalBulkSHA256ValidityWithResponse(context.Background(), test.body)
			if err != nil {
				t.Fatalf("validity request failed: %v", err)
			}
			if response.StatusCode() != http.StatusBadRequest || response.JSON400 == nil {
				t.Fatalf("validity status = %d, want %d: %s", response.StatusCode(), http.StatusBadRequest, response.Body)
			}
		})
	}
}
