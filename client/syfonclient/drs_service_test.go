package syfonclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		case r.Method == http.MethodGet && r.URL.Path == "/objects/checksum/abc":
			name := "hash.bin"
			resolved := []drsapi.DrsObject{{
				Id:        "did-hash",
				Name:      &name,
				Size:      12,
				Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}},
			}}
			writeJSON(t, w, http.StatusOK, drsapi.N200OkDrsObjects{ResolvedDrsObject: &resolved})
		case r.Method == http.MethodGet && r.URL.Path == "/objects/checksum/def":
			resolved := []drsapi.DrsObject{}
			writeJSON(t, w, http.StatusOK, drsapi.N200OkDrsObjects{ResolvedDrsObject: &resolved})
		case r.Method == http.MethodPost && r.URL.Path == "/objects/register":
			name := "registered.bin"
			writeJSON(t, w, http.StatusCreated, drsapi.N201ObjectsCreated{Objects: []drsapi.DrsObject{{Id: "obj-created", Name: &name, Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}}, CreatedTime: time.Now()}}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	recordName := "record.bin"
	recordSize := int64(77)
	recordUrls := []string{"https://storage.example/record.bin"}
	recordHashes := internalapi.HashInfo{"sha256": "abc123"}
	recordAuthz := map[string][]string{"org1": {"p1"}}
	records := []internalapi.InternalRecord{{Did: "did-record", Authorizations: &recordAuthz, FileName: &recordName, Size: &recordSize, Urls: &recordUrls, Hashes: &recordHashes}}
	listResp, err := json.Marshal(internalapi.ListRecordsResponse{Records: &records})
	if err != nil {
		t.Fatalf("marshal list response: %v", err)
	}
	requester := &fakeRequester{responseJSON: listResp}
	index := NewIndexService(mustInternalClient(t, server.URL), requester)
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
	query, err := url.ParseQuery(strings.TrimPrefix(requester.builder.Url, "/index?"))
	if err != nil {
		t.Fatalf("parse list query: %v", err)
	}
	if query.Get("limit") != "5" || query.Get("page") != "2" {
		t.Fatalf("unexpected list query values: %v", query)
	}
	if page.DrsObjects[0].AccessMethods == nil || len(*page.DrsObjects[0].AccessMethods) != 1 {
		t.Fatalf("expected mapped access methods, got %+v", page.DrsObjects[0])
	}
	method := (*page.DrsObjects[0].AccessMethods)[0]
	if method.Authorizations == nil || len(*method.Authorizations) == 0 {
		t.Fatalf("expected authz mapping, got %+v", method)
	}
	if _, ok := (*method.Authorizations)["org1"]; !ok {
		t.Fatalf("expected org1 in authz map, got %+v", *method.Authorizations)
	}

	projectPage, err := service.ListObjectsByProject(ctx, "proj-1", 10, 3)
	if err != nil || len(projectPage.DrsObjects) != 1 {
		t.Fatalf("ListObjectsByProject returned page=%+v err=%v", projectPage, err)
	}
	query, err = url.ParseQuery(strings.TrimPrefix(requester.builder.Url, "/index?"))
	if err != nil {
		t.Fatalf("parse project list query: %v", err)
	}
	if query.Get("project") != "proj-1" || query.Get("limit") != "10" || query.Get("page") != "3" {
		t.Fatalf("unexpected project list query values: %v", query)
	}

	sample, err := service.GetProjectSample(ctx, "proj-2", 4)
	if err != nil || len(sample.DrsObjects) != 1 {
		t.Fatalf("GetProjectSample returned page=%+v err=%v", sample, err)
	}
	query, err = url.ParseQuery(strings.TrimPrefix(requester.builder.Url, "/index?"))
	if err != nil {
		t.Fatalf("parse sample list query: %v", err)
	}
	if query.Get("project") != "proj-2" || query.Get("page") != "1" {
		t.Fatalf("unexpected sample query values: %v", query)
	}

	hashPage, err := service.BatchGetObjectsByHash(ctx, []string{"abc", "def"})
	if err != nil || len(hashPage.DrsObjects) != 1 || hashPage.DrsObjects[0].Id != "did-hash" {
		t.Fatalf("BatchGetObjectsByHash returned page=%+v err=%v", hashPage, err)
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

func TestDRSServiceResourceAuthzAndDelete(t *testing.T) {
	t.Parallel()

	recordName := "object.bin"
	recordSize := int64(77)
	recordURLs := []string{"https://storage.example/object.bin"}
	recordHashes := internalapi.HashInfo{"sha256": "abc"}
	authzProject := map[string][]string{"org1": {"proj1"}}
	authzOrg := map[string][]string{"org1": {}}
	projectMethods := []drsapi.AccessMethod{{
		Type:           "https",
		Authorizations: &authzProject,
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: recordURLs[0]},
	}}
	orgMethods := []drsapi.AccessMethod{{
		Type:           "https",
		Authorizations: &authzOrg,
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: recordURLs[0]},
	}}
	drsRecords := []drsapi.DrsObject{
		{
			Id:            "obj-1",
			Name:          &recordName,
			Size:          recordSize,
			Checksums:     []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}},
			AccessMethods: &projectMethods,
		},
		{
			Id:            "obj-2",
			Name:          &recordName,
			Size:          recordSize,
			Checksums:     []drsapi.Checksum{{Type: "sha256", Checksum: "abc"}},
			AccessMethods: &orgMethods,
		},
	}
	records := []internalapi.InternalRecord{
		{
			Did:            "obj-1",
			Authorizations: &authzProject,
			FileName:       &recordName,
			Size:           &recordSize,
			Urls:           &recordURLs,
			Hashes:         &recordHashes,
		},
		{
			Did:            "obj-2",
			Authorizations: &authzOrg,
			FileName:       &recordName,
			Size:           &recordSize,
			Urls:           &recordURLs,
			Hashes:         &recordHashes,
		},
	}
	listResp, err := json.Marshal(internalapi.ListRecordsResponse{Records: &records})
	if err != nil {
		t.Fatalf("marshal list response: %v", err)
	}
	requester := &fakeRequester{responseJSON: listResp}
	httpClient := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/objects/checksum/abc":
			return jsonResponse(http.StatusOK, drsapi.N200OkDrsObjects{ResolvedDrsObject: &drsRecords})
		case r.Method == http.MethodGet && r.URL.Path == "/objects/obj-1/access/https":
			return jsonResponse(http.StatusOK, drsapi.AccessURL{Url: "https://signed.example/object.bin"})
		case r.Method == http.MethodDelete && (r.URL.Path == "/index/obj-1" || r.URL.Path == "/index/obj-2"):
			return emptyResponse(http.StatusNoContent), nil
		default:
			return nil, fmt.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	})}
	index := NewIndexService(mustInternalClientWithHTTPClient(httpClient), requester)
	service := NewDRSService(mustDRSClientWithHTTPClient(httpClient), index)
	ctx := context.Background()

	matches, err := service.GetObjectsByHashForResource(ctx, "sha256:abc", "org1", "proj1")
	if err != nil {
		t.Fatalf("GetObjectsByHashForResource returned error: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("expected both scoped and org-wide records, got %+v", matches)
	}

	orgWideOnly, err := service.GetObjectsByHashForResource(ctx, "abc", "org1", "proj2")
	if err != nil {
		t.Fatalf("GetObjectsByHashForResource returned error: %v", err)
	}
	if len(orgWideOnly) != 1 || orgWideOnly[0].Id != "obj-2" {
		t.Fatalf("expected org-wide record only, got %+v", orgWideOnly)
	}

	if _, err := service.GetObjectsByHashForResource(ctx, "abc", "", "proj2"); err != nil {
		t.Fatalf("empty org should not error: %v", err)
	}
	if zero, err := service.GetObjectsByHashForResource(ctx, "abc", "", "proj2"); err != nil || len(zero) != 0 {
		t.Fatalf("expected no matches for empty org, got %+v err=%v", zero, err)
	}

	accessURL, err := service.ResolveResourceAccessURL(ctx, "sha256:abc", "org1", "proj1")
	if err != nil {
		t.Fatalf("ResolveResourceAccessURL returned error: %v", err)
	}
	if accessURL == nil || accessURL.Url != "https://signed.example/object.bin" {
		t.Fatalf("unexpected resolved access url: %+v", accessURL)
	}

	if err := service.DeleteRecordsByHash(ctx, "sha256:abc"); err != nil {
		t.Fatalf("DeleteRecordsByHash returned error: %v", err)
	}

	emptyHTTPClient := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && r.URL.Path == "/objects/checksum/abc" {
			empty := []drsapi.DrsObject{}
			return jsonResponse(http.StatusOK, drsapi.N200OkDrsObjects{ResolvedDrsObject: &empty})
		}
		return nil, fmt.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
	})}
	emptyRequester := &fakeRequester{responseJSON: []byte(`{"records":[]}`)}
	emptyIndex := NewIndexService(mustInternalClientWithHTTPClient(emptyHTTPClient), emptyRequester)
	emptyService := NewDRSService(mustDRSClientWithHTTPClient(emptyHTTPClient), emptyIndex)
	if err := emptyService.DeleteRecordsByHash(ctx, "sha256:abc"); err == nil {
		t.Fatal("expected no-records error from DeleteRecordsByHash")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func jsonResponse(status int, v any) (*http.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(string(body))),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

func emptyResponse(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

func mustInternalClientWithHTTPClient(client *http.Client) *internalapi.ClientWithResponses {
	c, err := internalapi.NewClientWithResponses("http://example.invalid", internalapi.WithHTTPClient(client))
	if err != nil {
		panic(err)
	}
	return c
}

func mustDRSClientWithHTTPClient(client *http.Client) *drsapi.ClientWithResponses {
	c, err := drsapi.NewClientWithResponses("http://example.invalid", drsapi.WithHTTPClient(client))
	if err != nil {
		panic(err)
	}
	return c
}
