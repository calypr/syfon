package internaldrs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
)

func TestHandleInternalInspectObjectScopedSuccess(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{Organization: "syfon", Project: "e2e", Key: "nested/file.bin", Scheme: "s3"})
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"b1": {Bucket: "b1"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|":    {Organization: "syfon", Bucket: "b1", PathPrefix: "program-root"},
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "b1", PathPrefix: "project-root"},
		},
	}
	storageFake := &internalDRSProbeFake{probeFn: func(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
		return []storage.ProbeResult{{Target: targets[0].Target, Metadata: storage.ObjectMetadata{Bucket: targets[0].Target.Bucket, Key: targets[0].Target.Key, Path: "file.bin", SizeBytes: 17, ETag: "etag-1", LastModified: time.Date(2026, 6, 11, 1, 2, 3, 0, time.UTC)}}}
	}}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Probe: storageFake})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon/project/e2e": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectObjectResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ObjectURL != "s3://b1/program-root/project-root/nested/file.bin" {
		t.Fatalf("unexpected object_url: %s", resp.ObjectURL)
	}
	if resp.Bucket != "b1" || resp.Key != "program-root/project-root/nested/file.bin" {
		t.Fatalf("unexpected location: bucket=%s key=%s", resp.Bucket, resp.Key)
	}
	if resp.ETag != "etag-1" || resp.SizeBytes != 17 {
		t.Fatalf("unexpected metadata: %+v", resp)
	}
	if resp.LastModTime != "2026-06-11T01:02:03Z" {
		t.Fatalf("unexpected last_modified: %s", resp.LastModTime)
	}
}

func TestHandleInternalInspectObjectRawSuccess(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{ObjectURL: "s3://b1/program-root/raw/file.bin"})
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{"b1": {Bucket: "b1"}},
		BucketScopes: map[string]buckets.Scope{
			"syfon|": {Organization: "syfon", Bucket: "b1", PathPrefix: "program-root"},
		},
	}
	storageFake := &internalDRSProbeFake{probeFn: func(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
		return []storage.ProbeResult{{Target: targets[0].Target, Metadata: storage.ObjectMetadata{Bucket: targets[0].Target.Bucket, Key: targets[0].Target.Key, Path: "file.bin", SizeBytes: 99, ETag: "etag-raw"}}}
	}}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Probe: storageFake})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "etag-raw") {
		t.Fatalf("expected raw metadata response, got %s", rr.Body.String())
	}
}

func TestHandleInternalInspectObjectMissingScope(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{Organization: "syfon", Project: "missing", Key: "nested/file.bin", Scheme: "s3"})
	om := newInternalDRSObjectManager(&testutils.MockDatabase{}, &internalDRSStorageFake{})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon/project/missing": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalInspectObjectPermissionDenied(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{ObjectURL: "s3://b1/program-root/raw/file.bin"})
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{"b1": {Bucket: "b1"}},
		BucketScopes: map[string]buckets.Scope{
			"syfon|": {Organization: "syfon", Bucket: "b1", PathPrefix: "program-root"},
		},
	}
	storageFake := &internalDRSProbeFake{probeFn: func(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
		return []storage.ProbeResult{{Target: targets[0].Target, Err: &storage.OperationError{Kind: storage.ErrorForbidden, Provider: "s3", Capability: "probe"}}}
	}}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Probe: storageFake})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalInspectObjectMalformedURL(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{ObjectURL: "https://example.com/file.bin"})
	om := newInternalDRSObjectManager(&testutils.MockDatabase{}, &internalDRSStorageFake{})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalInspectProjectRecords(t *testing.T) {
	body, _ := json.Marshal(internalInspectProjectRecordsRequest{Organization: "syfon", Project: "e2e"})
	name := "example.bin"
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	updated := time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
		Objects: map[string]*objects.Record{
			"obj-1": {
				Id:   "obj-1",
				Name: &name,
				Checksums: []objects.Checksum{
					{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				},
				CreatedTime: created,
				UpdatedTime: &updated,
				Size:        17,
				AccessMethods: &[]objects.AccessMethod{
					{
						Type:      "s3",
						AccessId:  ptr("acc-1"),
						AccessUrl: &objects.AccessURL{Url: "s3://bucket-a/prefix/example.bin"},
					},
				},
			},
		},
		ObjectAuthz: map[string]map[string][]string{
			"obj-1": {"syfon": {"e2e"}},
		},
	}
	om := newInternalDRSObjectManager(db, &internalDRSStorageFake{})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-records", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon/project/e2e": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectProjectRecordsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	item := resp.Items[0]
	if item.ObjectID != "obj-1" || item.Checksum != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("unexpected item identity: %+v", item)
	}
	if item.Name != "example.bin" {
		t.Fatalf("expected project record name, got %q", item.Name)
	}
	if item.Organization != "syfon" || item.Project != "e2e" {
		t.Fatalf("unexpected scope: %+v", item)
	}
	if len(item.AccessURLs) != 1 || item.AccessURLs[0] != "s3://bucket-a/prefix/example.bin" {
		t.Fatalf("unexpected access urls: %+v", item.AccessURLs)
	}
}

func TestHandleInternalInspectProjectRecordsPreservesLegacyDuplicatePhysicalRows(t *testing.T) {
	body, _ := json.Marshal(internalInspectProjectRecordsRequest{Organization: "syfon", Project: "e2e"})
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	newer := created.Add(time.Minute)
	urlFor := func(url string) *objects.AccessURL { return &objects.AccessURL{Url: url} }
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{
			"physical-a": {
				Id: "physical-a", Checksums: []objects.Checksum{{Type: "sha256", Checksum: checksum}},
				CreatedTime: created, UpdatedTime: &created,
				AccessMethods: &[]objects.AccessMethod{{Type: "s3", AccessUrl: urlFor("s3://bucket/physical-a")}},
			},
			"physical-b": {
				Id: "physical-b", Checksums: []objects.Checksum{{Type: "sha256", Checksum: checksum}},
				CreatedTime: newer, UpdatedTime: &newer,
				AccessMethods: &[]objects.AccessMethod{{Type: "s3", AccessUrl: urlFor("s3://bucket/physical-b")}},
			},
		},
		ObjectAuthz: map[string]map[string][]string{
			"physical-a": {"syfon": {"e2e"}},
			"physical-b": {"syfon": {"e2e"}},
		},
	}
	om := newInternalDRSObjectManager(db, &internalDRSStorageFake{})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-records", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon/project/e2e": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectProjectRecordsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected both physical rows, got %d: %+v", len(resp.Items), resp.Items)
	}
	byID := make(map[string]internalInspectProjectRecordItem, len(resp.Items))
	for _, item := range resp.Items {
		byID[item.ObjectID] = item
	}
	for id, wantURL := range map[string]string{
		"physical-a": "s3://bucket/physical-a",
		"physical-b": "s3://bucket/physical-b",
	} {
		item, ok := byID[id]
		if !ok {
			t.Fatalf("missing physical row %q: %+v", id, resp.Items)
		}
		if len(item.AccessURLs) != 1 || item.AccessURLs[0] != wantURL {
			t.Fatalf("physical row %q returned access URLs %v, want %q", id, item.AccessURLs, wantURL)
		}
	}
}

func TestProjectRecordMatchesAnyPathPrefixAvoidsFalsePrefixMatches(t *testing.T) {
	record := internalInspectProjectRecordItem{
		AccessURLs: []string{"s3://bucket-a/project-root/CONFIG/a.json"},
		AccessMethods: []internalProjectAccessMethod{
			{URL: "s3://bucket-a/project-root/CONFIG/a.json"},
		},
	}
	if !projectRecordMatchesAnyPathPrefix(record, "CONFIG", "project-root/CONFIG") {
		t.Fatalf("expected CONFIG path prefix to match CONFIG/a.json")
	}
	if projectRecordMatchesAnyPathPrefix(record, "CONFIGURATION", "project-root/CONFIGURATION") {
		t.Fatalf("expected CONFIGURATION path prefix not to match CONFIG/a.json")
	}
}

func TestHandleInternalInspectProjectScopes(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|":    {Organization: "syfon", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "program-root"},
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	om := newInternalDRSObjectManager(db, &internalDRSStorageFake{})
	authz := map[string]map[string]bool{
		"/organization/syfon":             {"read": true},
		"/organization/syfon/project/e2e": {"read": true},
	}
	cases := []struct {
		name   string
		method string
		path   string
		body   io.Reader
	}{
		{
			name:   "get query",
			method: http.MethodGet,
			path:   "/data/inspect/project-scopes?organization=syfon&project=e2e",
		},
		{
			name:   "post body",
			method: http.MethodPost,
			path:   "/data/inspect/project-scopes",
			body:   bytes.NewBufferString(`{"organization":"syfon","project":"e2e"}`),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := withTestAuthzContext(httptest.NewRequest(tc.method, tc.path, tc.body), "gen3", authz)
			rr := doInternalDRSTestRequest(req, om)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
			}
			var resp internalInspectProjectScopesResponse
			if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if len(resp.Items) != 2 {
				t.Fatalf("expected 2 items, got %d", len(resp.Items))
			}
			if resp.Items[0].Bucket != "bucket-a" || !strings.HasPrefix(resp.Items[0].Path, "s3://bucket-a/") {
				t.Fatalf("unexpected first scope: %+v", resp.Items[0])
			}
		})
	}
}

func TestHandleInternalInspectProjectBucketModesUsePrefixList(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|":    {Organization: "syfon", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "program-root"},
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	storageFake := &internalDRSInventoryFake{}
	var listCalls []storage.InventoryRequest
	var listedPrefixes []string
	storageFake.inventoryFn = func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		bucket := request.Target.Bucket
		prefix := request.Target.Prefix
		if bucket != "bucket-a" {
			t.Fatalf("unexpected list target bucket=%q prefix=%q", bucket, prefix)
		}
		listedPrefixes = append(listedPrefixes, prefix)
		listCalls = append(listCalls, request)
		if prefix == "program-root/project-root/CONFIG" {
			return storage.InventoryResult{Items: []storage.ObjectMetadata{
				{Provider: "s3", Bucket: bucket, Key: prefix + "/a.bin", Path: "CONFIG/a.bin", SizeBytes: 10},
				{Provider: "s3", Bucket: bucket, Key: prefix + "/nested/b.bin", Path: "CONFIG/nested/b.bin", SizeBytes: 15},
			}, Complete: true}, nil
		}
		if prefix != "program-root/project-root" {
			t.Fatalf("unexpected list prefix=%q", prefix)
		}
		return storage.InventoryResult{Items: []storage.ObjectMetadata{
			{Provider: "s3", Bucket: bucket, Key: prefix + "/a.bin", Path: "a.bin", SizeBytes: 10},
			{Provider: "s3", Bucket: bucket, Key: prefix + "/b.bin", Path: "b.bin", SizeBytes: 15},
		}, Complete: true}, nil
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})

	existsBody, _ := json.Marshal(internalInspectProjectBucketRequest{Organization: "syfon", Project: "e2e", Mode: "exists"})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-bucket", bytes.NewBuffer(existsBody)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var existsResp internalInspectProjectBucketResponse
	if err := json.NewDecoder(rr.Body).Decode(&existsResp); err != nil {
		t.Fatalf("decode exists response: %v", err)
	}
	if existsResp.Summary == nil || !existsResp.Summary.Exists || existsResp.Summary.Mode != "exists" {
		t.Fatalf("unexpected exists summary: %+v", existsResp.Summary)
	}
	if len(existsResp.Items) != 0 {
		t.Fatalf("exists mode should not return item rows, got %+v", existsResp.Items)
	}
	if len(listCalls) != 1 || listCalls[0].MaxKeys != 1 || listCalls[0].IncludeHead {
		t.Fatalf("exists mode should use MaxKeys=1 without HEAD, got %+v", listCalls)
	}

	summaryBody, _ := json.Marshal(internalInspectProjectBucketRequest{Organization: "syfon", Project: "e2e", Mode: "summary"})
	req = withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-bucket", bytes.NewBuffer(summaryBody)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr = doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var summaryResp internalInspectProjectBucketResponse
	if err := json.NewDecoder(rr.Body).Decode(&summaryResp); err != nil {
		t.Fatalf("decode summary response: %v", err)
	}
	if summaryResp.Summary == nil || summaryResp.Summary.Mode != "summary" || summaryResp.Summary.ObjectCount != 2 || summaryResp.Summary.TotalBytes != 25 {
		t.Fatalf("unexpected summary response: %+v", summaryResp.Summary)
	}
	if len(summaryResp.Items) != 0 {
		t.Fatalf("summary mode should not return item rows, got %+v", summaryResp.Items)
	}
	if len(listCalls) != 2 || listCalls[1].MaxKeys != 0 || listCalls[1].IncludeHead {
		t.Fatalf("summary mode should list recursively without HEAD, got %+v", listCalls)
	}

	itemsBody, _ := json.Marshal(internalInspectProjectBucketRequest{Organization: "syfon", Project: "e2e", Mode: "items", PathPrefix: "CONFIG"})
	req = withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-bucket", bytes.NewBuffer(itemsBody)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr = doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var itemsResp internalInspectProjectBucketResponse
	if err := json.NewDecoder(rr.Body).Decode(&itemsResp); err != nil {
		t.Fatalf("decode items response: %v", err)
	}
	if itemsResp.Summary == nil || itemsResp.Summary.Mode != "items" || itemsResp.Summary.Prefix != "program-root/project-root/CONFIG" {
		t.Fatalf("unexpected items summary: %+v", itemsResp.Summary)
	}
	if len(itemsResp.Items) != 2 {
		t.Fatalf("items mode should return recursive rows, got %+v", itemsResp.Items)
	}
	if len(listedPrefixes) != 3 || listedPrefixes[2] != "program-root/project-root/CONFIG" {
		t.Fatalf("expected path_prefix to scope recursive LIST, got %+v", listedPrefixes)
	}
}

func TestHandleInternalInspectProjectBucketInventoryListsProjectScope(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|":    {Organization: "syfon", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "program-root"},
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	storageFake := &internalDRSInventoryFake{}
	var listCalls []storage.InventoryRequest
	var listedPrefixes []string
	storageFake.inventoryFn = func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		bucket := request.Target.Bucket
		prefix := request.Target.Prefix
		if bucket != "bucket-a" {
			t.Fatalf("unexpected list target bucket=%q prefix=%q", bucket, prefix)
		}
		listedPrefixes = append(listedPrefixes, prefix)
		listCalls = append(listCalls, request)
		return storage.InventoryResult{Items: []storage.ObjectMetadata{
			{Provider: "s3", Bucket: bucket, Key: prefix + "/a.bin", Path: "CONFIG/a.bin", SizeBytes: 10},
			{Provider: "s3", Bucket: bucket, Key: prefix + "/nested/b.bin", Path: "CONFIG/nested/b.bin", SizeBytes: 15},
		}, Complete: true}, nil
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})

	body, _ := json.Marshal(internalInspectProjectBucketRequest{Organization: "syfon", Project: "e2e", PathPrefix: "CONFIG"})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-bucket/inventory", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectProjectBucketResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Summary == nil || resp.Summary.Mode != "items" || resp.Summary.Prefix != "program-root/project-root/CONFIG" {
		t.Fatalf("unexpected inventory summary: %+v", resp.Summary)
	}
	if resp.Summary.ObjectCount != 2 || resp.Summary.TotalBytes != 25 || len(resp.Items) != 2 {
		t.Fatalf("unexpected inventory response summary=%+v items=%+v", resp.Summary, resp.Items)
	}
	for _, item := range resp.Items {
		if !item.InventoryComplete {
			t.Fatalf("expected successful inventory item to be marked complete, got %+v", item)
		}
	}
	if len(listCalls) != 1 || listCalls[0].IncludeHead || listCalls[0].MaxKeys != 0 || listCalls[0].ExactPrefix {
		t.Fatalf("expected one recursive LIST without HEAD, got %+v", listCalls)
	}
	if len(listedPrefixes) != 1 || listedPrefixes[0] != "program-root/project-root/CONFIG" {
		t.Fatalf("expected path_prefix to scope recursive inventory, got %+v", listedPrefixes)
	}
}

func TestHandleInternalInspectProjectBucketInventoryReturnsPartialListing(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|":    {Organization: "syfon", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "program-root"},
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	storageFake := &internalDRSInventoryFake{inventoryFn: func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		return storage.InventoryResult{Items: []storage.ObjectMetadata{{
				Provider:  "s3",
				Bucket:    request.Target.Bucket,
				Key:       "program-root/project-root/observed.bin",
				Path:      "observed.bin",
				SizeBytes: 17,
			}}, Complete: false}, &storage.OperationError{
				Kind:       storage.ErrorIncomplete,
				Provider:   "s3",
				Capability: "inventory",
				Cause:      fmt.Errorf("terminal replay returned different page content"),
			}
	}}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})

	body, _ := json.Marshal(internalInspectProjectBucketRequest{Organization: "syfon", Project: "e2e"})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-bucket/inventory", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected partial inventory to return 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectProjectBucketResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Summary == nil || resp.Summary.InventoryComplete {
		t.Fatalf("expected incomplete inventory summary, got %+v", resp.Summary)
	}
	if !strings.Contains(resp.Summary.InventoryWarning, "terminal replay") {
		t.Fatalf("expected listing warning to be preserved, got %+v", resp.Summary)
	}
	if len(resp.Items) != 1 || resp.Items[0].InventoryComplete {
		t.Fatalf("expected observed partial item, got %+v", resp.Items)
	}
}

func TestHandleInternalDeleteProjectBucketObjectsPreservesPolicyOrderAndStatuses(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket-a": {Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", PathPrefix: "project-root"},
		},
	}
	storageFake := &internalDRSDeleteFake{}
	storageFake.deleteFn = func(_ context.Context, targets []storage.DeleteTarget) error {
		if len(targets) != 1 {
			t.Fatalf("expected one exact delete target per policy result, got %+v", targets)
		}
		if targets[0].Location == "s3://bucket-a/project-root/one.bin" {
			return fmt.Errorf("provider delete failed")
		}
		return nil
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Delete: storageFake})
	body, _ := json.Marshal(internalDeleteProjectBucketObjectsRequest{
		Organization: "syfon",
		Project:      "e2e",
		ObjectURLs: []string{
			" s3://bucket-a/project-root/one.bin ",
			"s3://bucket-a/project-root/one.bin",
			"s3://bucket-a/project-root/two.bin",
			"s3://bucket-a/other/outside.bin",
			"https://example.com/not-storage",
		},
	})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/project-bucket/delete", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"delete": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalDeleteProjectBucketObjectsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 4 {
		t.Fatalf("expected duplicate URL to be removed while preserving first-seen order, got %+v", resp.Items)
	}
	if resp.Items[0].ObjectURL != "s3://bucket-a/project-root/one.bin" || resp.Items[0].Status != "error" || !strings.Contains(resp.Items[0].Error, "provider delete failed") {
		t.Fatalf("unexpected first delete result: %+v", resp.Items[0])
	}
	if resp.Items[1].ObjectURL != "s3://bucket-a/project-root/two.bin" || resp.Items[1].Status != "deleted" {
		t.Fatalf("unexpected second delete result: %+v", resp.Items[1])
	}
	if resp.Items[2].Status != "forbidden" || resp.Items[3].Status != "invalid" {
		t.Fatalf("expected forbidden and invalid deferred results, got %+v", resp.Items)
	}
	if len(storageFake.deleteCalls) != 2 || storageFake.deleteCalls[0][0].Location != "s3://bucket-a/project-root/one.bin" || storageFake.deleteCalls[1][0].Location != "s3://bucket-a/project-root/two.bin" {
		t.Fatalf("unexpected physical delete calls: %+v", storageFake.deleteCalls)
	}
}

func TestHandleInternalInspectObjectBulkListValidatesExactKeyWithoutHead(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	storageFake := &internalDRSInventoryFake{}
	var listedPrefixesMu sync.Mutex
	var listedPrefixes []string
	storageFake.inventoryFn = func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		bucket := request.Target.Bucket
		prefix := request.Target.Prefix
		if bucket != "bucket-a" || !request.ExactPrefix || request.MaxKeys != 1 || request.IncludeHead {
			t.Fatalf("unexpected bulk-list options bucket=%q request=%+v", bucket, request)
		}
		listedPrefixesMu.Lock()
		listedPrefixes = append(listedPrefixes, prefix)
		listedPrefixesMu.Unlock()
		switch prefix {
		case "project-root/file.bin":
			return storage.InventoryResult{Items: []storage.ObjectMetadata{{Provider: "s3", Bucket: bucket, Key: "project-root/file.bin", Path: "file.bin", SizeBytes: 17, ETag: "etag-1", LastModified: time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC)}}, Complete: true}, nil
		case "project-root/dir":
			return storage.InventoryResult{Items: []storage.ObjectMetadata{{Provider: "s3", Bucket: bucket, Key: "project-root/dir/child.bin", Path: "dir/child.bin", SizeBytes: 19}}, Complete: true}, nil
		default:
			return storage.InventoryResult{Complete: true}, nil
		}
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})
	expectedSize := int64(17)
	body, _ := json.Marshal(internalInspectObjectsBulkRequest{Items: []internalInspectObjectRequest{
		{ID: "present", ObjectURL: "s3://bucket-a/project-root/file.bin", ExpectedSizeBytes: &expectedSize, ExpectedName: "file.bin"},
		{ID: "prefix-child", ObjectURL: "s3://bucket-a/project-root/dir", ExpectedSizeBytes: &expectedSize, ExpectedName: "dir"},
	}})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/bulk-list", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectObjectBulkResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 items, got %+v", resp.Items)
	}
	if !resp.Items[0].Exists || resp.Items[0].Status != "present" || resp.Items[0].ValidationStatus != "matched" {
		t.Fatalf("unexpected present item: %+v", resp.Items[0])
	}
	if resp.Items[1].Exists || resp.Items[1].Status != "not_found" {
		t.Fatalf("expected prefix child not to count as exact key, got %+v", resp.Items[1])
	}
	listedPrefixesMu.Lock()
	listedPrefixCount := len(listedPrefixes)
	listedPrefixesMu.Unlock()
	if listedPrefixCount != 2 {
		t.Fatalf("expected two LIST calls, got %+v", listedPrefixes)
	}
}

func TestHandleInternalInspectObjectBulkListDeduplicatesExactTargets(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	listCalls := 0
	storageFake := &internalDRSInventoryFake{}
	storageFake.inventoryFn = func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		listCalls++
		bucket := request.Target.Bucket
		prefix := request.Target.Prefix
		if bucket != "bucket-a" || prefix != "project-root/file.bin" || !request.ExactPrefix || request.MaxKeys != 1 {
			t.Fatalf("unexpected bulk-list request bucket=%q prefix=%q request=%+v", bucket, prefix, request)
		}
		return storage.InventoryResult{Items: []storage.ObjectMetadata{{Provider: "s3", Bucket: bucket, Key: "project-root/file.bin", Path: "file.bin", SizeBytes: 17}}, Complete: true}, nil
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})
	expectedSize := int64(17)
	body, _ := json.Marshal(internalInspectObjectsBulkRequest{Items: []internalInspectObjectRequest{
		{ID: "one", ObjectURL: "s3://bucket-a/project-root/file.bin", ExpectedSizeBytes: &expectedSize, ExpectedName: "file.bin"},
		{ID: "two", ObjectURL: "s3://bucket-a/project-root/file.bin", ExpectedSizeBytes: &expectedSize, ExpectedName: "file.bin"},
	}})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/bulk-list", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectObjectBulkResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if listCalls != 1 {
		t.Fatalf("expected duplicate exact keys to share one LIST call, got %d", listCalls)
	}
	if len(resp.Items) != 2 || resp.Items[0].ID != "one" || resp.Items[1].ID != "two" {
		t.Fatalf("expected one response per original request, got %+v", resp.Items)
	}
}

func TestHandleInternalInspectObjectBulkListSharesRemoteEvidenceAcrossValidationExpectations(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	listCalls := 0
	storageFake := &internalDRSInventoryFake{}
	storageFake.inventoryFn = func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		listCalls++
		return storage.InventoryResult{Items: []storage.ObjectMetadata{{Provider: "s3", Bucket: request.Target.Bucket, Key: "project-root/file.bin", Path: "file.bin", SizeBytes: 17}}, Complete: true}, nil
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})
	size17 := int64(17)
	size99 := int64(99)
	body, _ := json.Marshal(internalInspectObjectsBulkRequest{Items: []internalInspectObjectRequest{
		{ID: "matched", ObjectURL: "s3://bucket-a/project-root/file.bin", ExpectedSizeBytes: &size17, ExpectedName: "file.bin"},
		{ID: "mismatched", ObjectURL: "s3://bucket-a/project-root/file.bin", ExpectedSizeBytes: &size99, ExpectedName: "file.bin"},
	}})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/bulk-list", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectObjectBulkResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if listCalls != 1 {
		t.Fatalf("expected one shared LIST call, got %d", listCalls)
	}
	if len(resp.Items) != 2 || resp.Items[0].ValidationStatus != "matched" || resp.Items[1].ValidationStatus != "mismatched" {
		t.Fatalf("expected per-request validation after shared evidence, got %+v", resp.Items)
	}
}

func TestHandleInternalInspectObjectBulkListCoalescesDensePrefixes(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"cred-1": {CredentialID: "cred-1", Bucket: "bucket-a", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "bucket-a", CredentialID: "cred-1", PathPrefix: "project-root"},
		},
	}
	listCalls := 0
	storageFake := &internalDRSInventoryFake{}
	storageFake.inventoryFn = func(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
		listCalls++
		bucket := request.Target.Bucket
		prefix := request.Target.Prefix
		if bucket != "bucket-a" || prefix != "project-root/dense/" || !request.ExactPrefix || request.MaxKeys != 0 || request.IncludeHead {
			t.Fatalf("unexpected coalesced LIST request bucket=%q prefix=%q request=%+v", bucket, prefix, request)
		}
		out := make([]storage.ObjectMetadata, 0, 25)
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("project-root/dense/file-%02d.bin", i)
			out = append(out, storage.ObjectMetadata{Provider: "s3", Bucket: bucket, Key: key, Path: fmt.Sprintf("file-%02d.bin", i), SizeBytes: int64(i + 1)})
		}
		return storage.InventoryResult{Items: out, Complete: true}, nil
	}
	om := newInternalDRSObjectManager(db, internalDRSStorageCapabilities{Inventory: storageFake})
	items := make([]internalInspectObjectRequest, 0, 25)
	for i := 0; i < 25; i++ {
		size := int64(i + 1)
		items = append(items, internalInspectObjectRequest{
			ID:                fmt.Sprintf("item-%02d", i),
			ObjectURL:         fmt.Sprintf("s3://bucket-a/project-root/dense/file-%02d.bin", i),
			ExpectedSizeBytes: &size,
			ExpectedName:      fmt.Sprintf("file-%02d.bin", i),
		})
	}
	body, _ := json.Marshal(internalInspectObjectsBulkRequest{Items: items})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect/bulk-list", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{
		"/organization/syfon/project/e2e": {"read": true},
	})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectObjectBulkResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if listCalls != 1 {
		t.Fatalf("expected dense prefix to coalesce into one LIST call, got %d", listCalls)
	}
	if len(resp.Items) != 25 {
		t.Fatalf("expected 25 items, got %d", len(resp.Items))
	}
}
