package drs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/calypr/syfon/client/pkg/request"
)

func countByHashCaseInsensitive(m map[string][]DRSObject, target string) int {
	total := 0
	for k, v := range m {
		if strings.EqualFold(k, target) {
			total += len(v)
		}
	}
	return total
}

type requestSpy struct {
	lastMethod string
	lastURL    string
	lastBody   []byte
	doFunc     func(method, url string, body []byte) (*http.Response, error)
}

func (s *requestSpy) New(method, url string) *request.RequestBuilder {
	s.lastMethod = method
	s.lastURL = url
	return &request.RequestBuilder{
		Method:  method,
		Url:     url,
		Headers: map[string]string{},
	}
}

func (s *requestSpy) Do(ctx context.Context, req *request.RequestBuilder) (*http.Response, error) {
	_ = ctx
	if req != nil && req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		s.lastBody = b
	}
	if s.doFunc != nil {
		return s.doFunc(req.Method, req.Url, s.lastBody)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}, nil
}

func (s *requestSpy) DoJSON(ctx context.Context, rb *request.RequestBuilder, out any) error {
	resp, err := s.Do(ctx, rb)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func TestBatchGetObjectsByHash_UsesBulkHashesEndpoint(t *testing.T) {
	spy := &requestSpy{}
	spy.doFunc = func(method, url string, body []byte) (*http.Response, error) {
		if method != http.MethodPost {
			t.Fatalf("expected POST, got %s", method)
		}
		if !strings.HasSuffix(url, "/index/bulk/hashes") {
			t.Fatalf("expected /index/bulk/hashes endpoint, got %s", url)
		}

		var req internalapi.BulkHashesRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if got, want := len(req.Hashes), 2; got != want {
			t.Fatalf("unexpected number of hashes in request: got=%d want=%d", got, want)
		}
		if !strings.EqualFold(req.Hashes[0], "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") {
			t.Fatalf("unexpected first hash query: %s", req.Hashes[0])
		}

		did := "did-1"
		fileName := "file.bin"
		hashes := map[string]string{"sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		size := int64(12)
		h := internalapi.HashInfo(hashes)
		urls := []string{"s3://bucket/path"}
		resp := internalapi.ListRecordsResponse{
			Records: &[]internalapi.InternalRecord{{
				Did:      did,
				FileName: &fileName,
				Hashes:   &h,
				Size:     &size,
				Urls:     &urls,
			}},
		}
		respBody, _ := json.Marshal(resp)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(string(respBody))),
		}, nil
	}

	c := NewLocalDrsClient(spy, "http://example.org", nil)
	out, err := c.BatchGetObjectsByHash(context.Background(), []string{
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	})
	if err != nil {
		t.Fatalf("BatchGetObjectsByHash returned error: %v", err)
	}
	if got := countByHashCaseInsensitive(out, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); got != 1 {
		t.Fatalf("expected 1 record for first hash, got %d (keys=%v)", got, mapKeys(out))
	}
	if got := countByHashCaseInsensitive(out, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); got != 0 {
		t.Fatalf("expected empty result for second hash, got %d", got)
	}
}

func mapKeys(m map[string][]DRSObject) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestRegisterRecords_UsesBulkCreateEndpoint(t *testing.T) {
	spy := &requestSpy{}
	spy.doFunc = func(method, url string, body []byte) (*http.Response, error) {
		if method != http.MethodPost {
			t.Fatalf("expected POST, got %s", method)
		}
		if !strings.HasSuffix(url, "/index/bulk") {
			t.Fatalf("expected /index/bulk endpoint, got %s", url)
		}

		var req internalapi.BulkCreateRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if got, want := len(req.Records), 1; got != want {
			t.Fatalf("unexpected number of records in request: got=%d want=%d", got, want)
		}

		did := "did-bulk-1"
		fileName := "bulk.bin"
		hashes := map[string]string{"sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
		size := int64(21)
		h := internalapi.HashInfo(hashes)
		urls := []string{"s3://bucket/bulk.bin"}
		resp := internalapi.ListRecordsResponse{
			Records: &[]internalapi.InternalRecord{{
				Did:      did,
				FileName: &fileName,
				Hashes:   &h,
				Size:     &size,
				Urls:     &urls,
			}},
		}
		respBody, _ := json.Marshal(resp)
		return &http.Response{
			StatusCode: http.StatusCreated,
			Status:     "201 Created",
			Body:       io.NopCloser(strings.NewReader(string(respBody))),
		}, nil
	}

	c := NewLocalDrsClient(spy, "http://example.org", nil)
	fileName := "bulk.bin"
	accessURL := struct {
		Headers *[]string `json:"headers,omitempty"`
		Url     string    `json:"url"`
	}{Url: "s3://bucket/bulk.bin"}
	accessMethods := []AccessMethod{{
		Type:      "s3",
		AccessUrl: &accessURL,
	}}
	in := []*DRSObject{{
		Id:            "did-bulk-1",
		Name:          &fileName,
		Size:          21,
		Checksums: []Checksum{{
			Type:     string(hash.ChecksumTypeSHA256),
			Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		}},
		AccessMethods: &accessMethods,
	}}

	out, err := c.RegisterRecords(context.Background(), in)
	if err != nil {
		t.Fatalf("RegisterRecords returned error: %v", err)
	}
	if got, want := len(out), 1; got != want {
		t.Fatalf("expected %d response records, got %d", want, got)
	}
	if out[0].Id != "did-bulk-1" {
		t.Fatalf("unexpected returned did: %s", out[0].Id)
	}
}
