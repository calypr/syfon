package gen3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	datahash "github.com/calypr/data-client/hash"
	dataindexd "github.com/calypr/data-client/indexd"
	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
	"github.com/gorilla/mux"
)

func newGen3Router(t *testing.T) (*mux.Router, *testutils.MockDatabase) {
	t.Helper()
	db := &testutils.MockDatabase{}
	router := mux.NewRouter()
	RegisterGen3Routes(router, db)
	return router, db
}

func TestIndexdCreateGetAndUpdate(t *testing.T) {
	router, _ := newGen3Router(t)

	create := IndexdRecord{
		IndexdRecord: dataindexd.IndexdRecord{
			Did:      "sha-a",
			Size:     10,
			Hashes:   datahash.HashInfo{SHA256: "sha-a"},
			URLs:     []string{"s3://bucket/a"},
			Authz:    []string{"/programs/p/projects/q"},
			FileName: "a.bin",
		},
	}
	body, _ := json.Marshal(create)
	req := httptest.NewRequest(http.MethodPost, "/index/index", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", rr.Code, rr.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/index/index/sha-a", nil)
	getRR := httptest.NewRecorder()
	router.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", getRR.Code, getRR.Body.String())
	}
	var got IndexdRecordResponse
	if err := json.NewDecoder(getRR.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Did != "sha-a" || got.Hashes.SHA256 != "sha-a" {
		t.Fatalf("unexpected get record: %+v", got)
	}

	update := IndexdRecord{
		IndexdRecord: dataindexd.IndexdRecord{
			Did:      "sha-a",
			Size:     42,
			Hashes:   datahash.HashInfo{SHA256: "sha-a", MD5: "deadbeef"},
			URLs:     []string{"s3://bucket/a-new"},
			Authz:    []string{"/programs/new/projects/new"},
			FileName: "a-new.bin",
		},
	}
	updateBody, _ := json.Marshal(update)
	updateReq := httptest.NewRequest(http.MethodPut, "/index/index/sha-a", bytes.NewReader(updateBody))
	updateRR := httptest.NewRecorder()
	router.ServeHTTP(updateRR, updateReq)
	if updateRR.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", updateRR.Code, updateRR.Body.String())
	}

	getReq2 := httptest.NewRequest(http.MethodGet, "/index/index/sha-a", nil)
	getRR2 := httptest.NewRecorder()
	router.ServeHTTP(getRR2, getReq2)
	if getRR2.Code != http.StatusOK {
		t.Fatalf("get after update status=%d body=%s", getRR2.Code, getRR2.Body.String())
	}
	var got2 IndexdRecordResponse
	if err := json.NewDecoder(getRR2.Body).Decode(&got2); err != nil {
		t.Fatal(err)
	}
	if got2.Size != 42 || got2.FileName != "a-new.bin" {
		t.Fatalf("unexpected updated fields: %+v", got2)
	}
	if len(got2.URLs) != 1 || got2.URLs[0] != "s3://bucket/a-new" {
		t.Fatalf("unexpected urls after update: %+v", got2.URLs)
	}
	if len(got2.Authz) != 1 || got2.Authz[0] != "/programs/new/projects/new" {
		t.Fatalf("unexpected authz after update: %+v", got2.Authz)
	}
	if got2.Hashes.MD5 != "deadbeef" {
		t.Fatalf("expected md5 checksum after update, got: %+v", got2.Hashes)
	}
}

func TestIndexdBulkHashesAndDocuments(t *testing.T) {
	router, _ := newGen3Router(t)

	for _, id := range []string{"sha-b", "sha-c"} {
		rec := IndexdRecord{
			IndexdRecord: dataindexd.IndexdRecord{
				Did:      id,
				Size:     5,
				Hashes:   datahash.HashInfo{SHA256: id},
				URLs:     []string{"s3://bucket/" + id},
				Authz:    []string{"/programs/p/projects/q"},
				FileName: id + ".bin",
			},
		}
		body, _ := json.Marshal(rec)
		req := httptest.NewRequest(http.MethodPost, "/index/index", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("create %s failed status=%d body=%s", id, rr.Code, rr.Body.String())
		}
	}

	hashReqBody, _ := json.Marshal(map[string][]string{
		"hashes": []string{"sha256:sha-b", "sha-c"},
	})
	hashReq := httptest.NewRequest(http.MethodPost, "/index/index/bulk/hashes", bytes.NewReader(hashReqBody))
	hashRR := httptest.NewRecorder()
	router.ServeHTTP(hashRR, hashReq)
	if hashRR.Code != http.StatusOK {
		t.Fatalf("bulk hashes status=%d body=%s", hashRR.Code, hashRR.Body.String())
	}
	var hashesResp ListRecordsResponse
	if err := json.NewDecoder(hashRR.Body).Decode(&hashesResp); err != nil {
		t.Fatal(err)
	}
	if len(hashesResp.Records) != 2 {
		t.Fatalf("expected 2 records from bulk hashes, got %d", len(hashesResp.Records))
	}
	dids := []string{hashesResp.Records[0].Did, hashesResp.Records[1].Did}
	sort.Strings(dids)
	if dids[0] != "sha-b" || dids[1] != "sha-c" {
		t.Fatalf("unexpected dids from bulk hashes: %+v", dids)
	}

	docReqBody, _ := json.Marshal(map[string][]string{"ids": []string{"sha-c"}})
	docReq := httptest.NewRequest(http.MethodPost, "/bulk/documents", bytes.NewReader(docReqBody))
	docRR := httptest.NewRecorder()
	router.ServeHTTP(docRR, docReq)
	if docRR.Code != http.StatusOK {
		t.Fatalf("bulk documents status=%d body=%s", docRR.Code, docRR.Body.String())
	}
	var docs []IndexdRecordResponse
	if err := json.NewDecoder(docRR.Body).Decode(&docs); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0].Did != "sha-c" {
		t.Fatalf("unexpected bulk documents response: %+v", docs)
	}
}

func TestIndexdBulkSHA256Validity(t *testing.T) {
	router, db := newGen3Router(t)
	db.Credentials = map[string]core.S3Credential{
		"valid-bucket": {Bucket: "valid-bucket", Region: "us-east-1"},
	}
	db.Objects = map[string]*drs.DrsObject{
		"sha-valid": {
			Id: "sha-valid",
			AccessMethods: []drs.AccessMethod{
				{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://valid-bucket/path/to/object"}},
			},
		},
		"sha-bad-bucket": {
			Id: "sha-bad-bucket",
			AccessMethods: []drs.AccessMethod{
				{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://missing-bucket/path/to/object"}},
			},
		},
		"sha-bad-url": {
			Id: "sha-bad-url",
			AccessMethods: []drs.AccessMethod{
				{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "https://valid-bucket/path/to/object"}},
			},
		},
	}

	reqBody, _ := json.Marshal(map[string][]string{
		"sha256": []string{"sha-valid", "sha-bad-bucket", "sha-bad-url", "sha-missing"},
	})
	req := httptest.NewRequest(http.MethodPost, "/index/index/bulk/sha256/validity", bytes.NewReader(reqBody))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk sha validity status=%d body=%s", rr.Code, rr.Body.String())
	}

	var resp map[string]bool
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp["sha-valid"] {
		t.Fatalf("expected sha-valid to be true: %+v", resp)
	}
	if resp["sha-bad-bucket"] {
		t.Fatalf("expected sha-bad-bucket to be false: %+v", resp)
	}
	if resp["sha-bad-url"] {
		t.Fatalf("expected sha-bad-url to be false: %+v", resp)
	}
	if resp["sha-missing"] {
		t.Fatalf("expected sha-missing to be false: %+v", resp)
	}
}

func TestIndexdBulkSHA256Validity_AcceptsHashesAlias(t *testing.T) {
	router, db := newGen3Router(t)
	db.Credentials = map[string]core.S3Credential{
		"valid-bucket": {Bucket: "valid-bucket", Region: "us-east-1"},
	}
	db.Objects = map[string]*drs.DrsObject{
		"sha-prefixed": {
			Id: "sha-prefixed",
			AccessMethods: []drs.AccessMethod{
				{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://valid-bucket/key"}},
			},
		},
	}

	reqBody, _ := json.Marshal(map[string][]string{
		"hashes": []string{"sha256:sha-prefixed"},
	})
	req := httptest.NewRequest(http.MethodPost, "/index/index/bulk/sha256/validity", bytes.NewReader(reqBody))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bulk sha validity status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]bool
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp["sha-prefixed"] {
		t.Fatalf("expected prefixed hash to be normalized and valid: %+v", resp)
	}
}

func TestIndexdGetUnauthorizedStatusCodes(t *testing.T) {
	db := &testutils.MockDatabase{GetObjectErr: core.ErrUnauthorized}
	router := mux.NewRouter()
	RegisterGen3Routes(router, db)

	req401 := httptest.NewRequest(http.MethodGet, "/index/index/sha-denied", nil)
	ctx401 := context.WithValue(req401.Context(), core.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, core.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	router.ServeHTTP(rr401, req401)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when auth header missing, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403 := httptest.NewRequest(http.MethodGet, "/index/index/sha-denied", nil)
	ctx403 := context.WithValue(req403.Context(), core.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, core.AuthHeaderPresentKey, true)
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	router.ServeHTTP(rr403, req403)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when auth header present but unauthorized, got %d body=%s", rr403.Code, rr403.Body.String())
	}
}

func TestIndexdBulkCreateGen3Unauthorized(t *testing.T) {
	router, _ := newGen3Router(t)
	body, _ := json.Marshal(IndexdBulkCreateRequest{
		Records: []IndexdRecord{
			{
				IndexdRecord: dataindexd.IndexdRecord{
					Did:      "sha-z",
					Hashes:   datahash.HashInfo{SHA256: "sha-z"},
					Authz:    []string{"/programs/p/projects/q"},
					FileName: "z.bin",
				},
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/index/index/bulk", bytes.NewReader(body))
	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthorized bulk create, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestIndexdListAndDeleteByOrganizationProject(t *testing.T) {
	router, db := newGen3Router(t)
	db.Objects = map[string]*drs.DrsObject{
		"sha-1": {
			Id:             "sha-1",
			Authorizations: []string{"/programs/cbds/projects/p1"},
			Checksums:      []drs.Checksum{{Type: "sha256", Checksum: "sha-1"}},
		},
		"sha-2": {
			Id:             "sha-2",
			Authorizations: []string{"/programs/cbds/projects/p2"},
			Checksums:      []drs.Checksum{{Type: "sha256", Checksum: "sha-2"}},
		},
	}

	getReq := httptest.NewRequest(http.MethodGet, "/index/index?organization=cbds&project=p1", nil)
	getCtx := context.WithValue(getReq.Context(), core.AuthModeKey, "gen3")
	getCtx = context.WithValue(getCtx, core.AuthHeaderPresentKey, true)
	getCtx = context.WithValue(getCtx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/p1": {"read": true},
	})
	getReq = getReq.WithContext(getCtx)
	getRR := httptest.NewRecorder()
	router.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", getRR.Code, getRR.Body.String())
	}
	var listResp ListRecordsResponse
	if err := json.NewDecoder(getRR.Body).Decode(&listResp); err != nil {
		t.Fatal(err)
	}
	if len(listResp.Records) != 1 || listResp.Records[0].Did != "sha-1" {
		t.Fatalf("unexpected list response: %+v", listResp.Records)
	}
	if listResp.Records[0].Organization != "cbds" || listResp.Records[0].Project != "p1" {
		t.Fatalf("expected organization/project projection, got %+v", listResp.Records[0])
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/index/index?organization=cbds&project=p1", nil)
	delCtx := context.WithValue(delReq.Context(), core.AuthModeKey, "gen3")
	delCtx = context.WithValue(delCtx, core.AuthHeaderPresentKey, true)
	delCtx = context.WithValue(delCtx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/p1": {"delete": true, "read": true},
	})
	delReq = delReq.WithContext(delCtx)
	delRR := httptest.NewRecorder()
	router.ServeHTTP(delRR, delReq)
	if delRR.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", delRR.Code, delRR.Body.String())
	}
	if _, ok := db.Objects["sha-1"]; ok {
		t.Fatal("expected sha-1 to be deleted")
	}
	if _, ok := db.Objects["sha-2"]; !ok {
		t.Fatal("expected sha-2 to remain")
	}
}

func TestIndexdDeleteByID(t *testing.T) {
	router, db := newGen3Router(t)
	db.Objects = map[string]*drs.DrsObject{
		"sha-del": {Id: "sha-del"},
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/index/index/sha-del", nil)
	delReq = mux.SetURLVars(delReq, map[string]string{"id": "sha-del"})
	delRR := httptest.NewRecorder()
	router.ServeHTTP(delRR, delReq)
	if delRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", delRR.Code, delRR.Body.String())
	}
}

func TestParseScopeQuery(t *testing.T) {
	req1 := httptest.NewRequest(http.MethodGet, "/index/index?authz=/programs/a/projects/b", nil)
	scope, ok, err := parseScopeQuery(req1)
	if err != nil || !ok || scope != "/programs/a/projects/b" {
		t.Fatalf("unexpected authz parse result: scope=%q ok=%v err=%v", scope, ok, err)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/index/index?organization=a&project=b", nil)
	scope, ok, err = parseScopeQuery(req2)
	if err != nil || !ok || scope != "/programs/a/projects/b" {
		t.Fatalf("unexpected org/project parse result: scope=%q ok=%v err=%v", scope, ok, err)
	}

	req3 := httptest.NewRequest(http.MethodGet, "/index/index?project=b", nil)
	_, _, err = parseScopeQuery(req3)
	if err == nil {
		t.Fatal("expected error when project provided without organization")
	}
}

func TestIndexdListNotImplementedWithoutQuery(t *testing.T) {
	router, _ := newGen3Router(t)
	req := httptest.NewRequest(http.MethodGet, "/index/index", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestWriteDBErrorBranches(t *testing.T) {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)
	writeDBError(w, req, core.ErrUnauthorized)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}

	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	writeDBError(w2, req2, core.ErrNotFound)
	if w2.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w2.Code)
	}

	w3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	writeDBError(w3, req3, errors.New("boom"))
	if w3.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w3.Code)
	}
}

func TestCanonicalIDAndIndexdToDrsHelpers(t *testing.T) {
	if got := canonicalIDFromIndexd(IndexdRecord{IndexdRecord: dataindexd.IndexdRecord{Did: "did"}}); got != "did" {
		t.Fatalf("expected did, got %q", got)
	}
	if got := canonicalIDFromIndexd(IndexdRecord{IndexdRecord: dataindexd.IndexdRecord{Hashes: datahash.HashInfo{SHA256: "abc"}}}); got != "abc" {
		t.Fatalf("expected sha256 fallback, got %q", got)
	}
	if got := canonicalIDFromIndexd(IndexdRecord{}); got != "" {
		t.Fatalf("expected empty id, got %q", got)
	}

	if _, err := indexdToDrs(IndexdRecord{}); err == nil {
		t.Fatal("expected validation error for missing did/hash")
	}
	obj, err := indexdToDrs(IndexdRecord{
		IndexdRecord: dataindexd.IndexdRecord{Hashes: datahash.HashInfo{SHA256: "x"}},
		Organization: "cbds",
		Project:      "p1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obj.Id != "x" || len(obj.Authorizations) != 1 || obj.Authorizations[0] != "/programs/cbds/projects/p1" {
		t.Fatalf("unexpected mapping output: obj=%+v", obj)
	}
}

func TestIndexdToDrsWithScopeFromEmbeddedRecord(t *testing.T) {
	obj, err := indexdToDrs(IndexdRecord{
		IndexdRecord: dataindexd.IndexdRecord{
			Hashes: datahash.HashInfo{SHA256: "x2"},
			URLs:   []string{"s3://bucket/x2"},
		},
		Organization: "cbds",
		Project:      "p1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obj.Id != "x2" || len(obj.Authorizations) != 1 || obj.Authorizations[0] != "/programs/cbds/projects/p1" {
		t.Fatalf("unexpected mapping output: obj=%+v", obj)
	}
}

func TestIndexdBulkCreateAndDocsValidationErrors(t *testing.T) {
	router, _ := newGen3Router(t)

	badBulk := httptest.NewRequest(http.MethodPost, "/index/index/bulk", bytes.NewBufferString(`{}`))
	badBulkRR := httptest.NewRecorder()
	router.ServeHTTP(badBulkRR, badBulk)
	if badBulkRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty bulk create, got %d", badBulkRR.Code)
	}

	badDocs := httptest.NewRequest(http.MethodPost, "/bulk/documents", bytes.NewBufferString(`{`))
	badDocsRR := httptest.NewRecorder()
	router.ServeHTTP(badDocsRR, badDocs)
	if badDocsRR.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed docs body, got %d", badDocsRR.Code)
	}
}

func TestIndexdListByHashPath(t *testing.T) {
	router, db := newGen3Router(t)
	db.Objects = map[string]*drs.DrsObject{
		"sha-h": {
			Id:        "sha-h",
			Checksums: []drs.Checksum{{Type: "sha256", Checksum: "sha-h"}},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/index/index?hash=sha256:sha-h", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
}
