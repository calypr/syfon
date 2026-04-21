package syfonclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	internalapi "github.com/calypr/syfon/apigen/client/internalapi"
)

func TestDRSServiceResolveAndList(t *testing.T) {
	t.Parallel()

	var lastIndexQuery url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/objects/obj-1":
			name := "object.bin"
			accessID := "acc-1"
			accessMethods := []drsapi.AccessMethod{{
				AccessId: &accessID,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "https://signed.example/object.bin"},
				Type: drsapi.AccessMethodType("https"),
			}}
			writeJSON(t, w, http.StatusOK, drsapi.DrsObject{Id: "obj-1", Name: &name, Size: 99, Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}}, CreatedTime: time.Now(), AccessMethods: &accessMethods})
		case r.Method == http.MethodGet && r.URL.Path == "/objects/no-access":
			writeJSON(t, w, http.StatusOK, drsapi.DrsObject{Id: "no-access", Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}}, CreatedTime: time.Now()})
		case r.Method == http.MethodGet && r.URL.Path == "/objects/obj-1/access/acc-1":
			writeJSON(t, w, http.StatusOK, drsapi.AccessURL{Url: "https://signed.example/access"})
		case r.Method == http.MethodPost && r.URL.Path == "/objects/register":
			name := "registered.bin"
			writeJSON(t, w, http.StatusCreated, drsapi.N201ObjectsCreated{Objects: []drsapi.DrsObject{{Id: "obj-created", Name: &name, Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}}, CreatedTime: time.Now()}}})
		case r.Method == http.MethodGet && r.URL.Path == "/index":
			lastIndexQuery = r.URL.Query()
			name := "record.bin"
			size := int64(77)
			urls := []string{"https://storage.example/record.bin"}
			hashes := internalapi.HashInfo{"sha256": "abc123"}
			records := []internalapi.InternalRecord{{Did: "did-record", Authz: []string{"/programs/p1"}, FileName: &name, Size: &size, Urls: &urls, Hashes: &hashes}}
			writeJSON(t, w, http.StatusOK, internalapi.ListRecordsResponse{Records: &records})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	index := NewIndexService(mustInternalClient(t, server.URL), &fakeRequester{})
	service := NewDRSService(mustDRSClient(t, server.URL), index)
	ctx := context.Background()

	resolved, err := service.Resolve(ctx, "obj-1")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if resolved.Id != "obj-1" || resolved.Name != "object.bin" || resolved.ProviderURL != "https://signed.example/object.bin" || resolved.AccessMethod != "https" {
		t.Fatalf("unexpected resolved object: %+v", resolved)
	}

	if _, err := service.Resolve(ctx, "no-access"); err == nil || !strings.Contains(err.Error(), "no access methods found") {
		t.Fatalf("expected missing access methods error, got %v", err)
	}

	obj, err := service.GetObject(ctx, "obj-1")
	if err != nil || obj.Id != "obj-1" {
		t.Fatalf("GetObject returned obj=%+v err=%v", obj, err)
	}

	page, err := service.ListObjects(ctx, 5, 2)
	if err != nil || len(page.DrsObjects) != 1 || page.DrsObjects[0].Id != "did-record" {
		t.Fatalf("ListObjects returned page=%+v err=%v", page, err)
	}
	if lastIndexQuery.Get("limit") != "5" || lastIndexQuery.Get("page") != "2" {
		t.Fatalf("unexpected list query values: %v", lastIndexQuery)
	}
	if page.DrsObjects[0].AccessMethods == nil || len(*page.DrsObjects[0].AccessMethods) != 1 {
		t.Fatalf("expected mapped access methods, got %+v", page.DrsObjects[0])
	}
	method := (*page.DrsObjects[0].AccessMethods)[0]
	if method.Authorizations == nil || method.Authorizations.BearerAuthIssuers == nil || len(*method.Authorizations.BearerAuthIssuers) != 1 {
		t.Fatalf("expected authz mapping, got %+v", method)
	}

	projectPage, err := service.ListObjectsByProject(ctx, "proj-1", 10, 3)
	if err != nil || len(projectPage.DrsObjects) != 1 {
		t.Fatalf("ListObjectsByProject returned page=%+v err=%v", projectPage, err)
	}
	if lastIndexQuery.Get("project") != "proj-1" || lastIndexQuery.Get("limit") != "10" || lastIndexQuery.Get("page") != "3" {
		t.Fatalf("unexpected project list query values: %v", lastIndexQuery)
	}

	sample, err := service.GetProjectSample(ctx, "proj-2", 4)
	if err != nil || len(sample.DrsObjects) != 1 {
		t.Fatalf("GetProjectSample returned page=%+v err=%v", sample, err)
	}
	if lastIndexQuery.Get("project") != "proj-2" || lastIndexQuery.Get("page") != "1" {
		t.Fatalf("unexpected sample query values: %v", lastIndexQuery)
	}

	hashPage, err := service.BatchGetObjectsByHash(ctx, []string{"abc", "def"})
	if err != nil || len(hashPage.DrsObjects) != 1 {
		t.Fatalf("BatchGetObjectsByHash returned page=%+v err=%v", hashPage, err)
	}
	if lastIndexQuery.Get("hash") != "abc,def" {
		t.Fatalf("expected joined hash query, got %v", lastIndexQuery)
	}

	accessURL, err := service.GetAccessURL(ctx, "obj-1", "acc-1")
	if err != nil || accessURL.Url != "https://signed.example/access" {
		t.Fatalf("GetAccessURL returned accessURL=%+v err=%v", accessURL, err)
	}

	registered, err := service.RegisterObjects(ctx, drsapi.RegisterObjectsJSONRequestBody{Candidates: []drsapi.DrsObjectCandidate{{Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}}}}})
	if err != nil || len(registered.Objects) != 1 || registered.Objects[0].Id != "obj-created" {
		t.Fatalf("RegisterObjects returned registered=%+v err=%v", registered, err)
	}
}

