package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
)

func TestRunPaginatesAndLoadsAllRecords(t *testing.T) {
	size := int64(1)
	source := &fakeSource{pages: [][]IndexdRecord{
		{
			{DID: "a", Size: &size, Hashes: map[string]string{"sha256": "sha-a"}},
			{DID: "b", Size: &size, Hashes: map[string]string{"sha256": "sha-b"}},
		},
		{
			{DID: "c", Size: &size, Hashes: map[string]string{"sha256": "sha-c"}},
		},
	}}
	loader := &fakeLoader{}

	stats, err := Run(context.Background(), source, loader, Config{BatchSize: 2, Sweeps: 1})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched != 3 || stats.Loaded != 3 || stats.CountOfUniqueIDs != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if len(source.starts) != 2 || source.starts[0] != "" || source.starts[1] != "b" {
		t.Fatalf("unexpected pagination starts: %+v", source.starts)
	}
	if len(loader.records) != 3 {
		t.Fatalf("expected 3 loaded records, got %d", len(loader.records))
	}
}

func TestRunDryRunDoesNotRequireLoader(t *testing.T) {
	size := int64(1)
	source := &fakeSource{pages: [][]IndexdRecord{{
		{DID: "a", Size: &size, Hashes: map[string]string{"sha256": "sha-a"}},
	}}}

	stats, err := Run(context.Background(), source, nil, Config{BatchSize: 100, DryRun: true})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched != 1 || stats.Loaded != 1 || stats.CountOfUniqueIDs != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestHTTPClientEndToEndWithMockIndexdAndSyfon(t *testing.T) {
	size := int64(7)
	var loaded struct {
		Records []struct {
			Did              string   `json:"did"`
			ControlledAccess []string `json:"controlled_access"`
			AccessMethods    []struct {
				Type      string `json:"type"`
				AccessUrl struct {
					Url string `json:"url"`
				} `json:"access_url"`
			} `json:"access_methods"`
		} `json:"records"`
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/index" {
			t.Fatalf("unexpected source path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("form"); got != "" {
			t.Fatalf("did not expect form query, got %q", got)
		}
		_ = json.NewEncoder(w).Encode(IndexdPage{Records: []IndexdRecord{{
			DID:    "dg.test/1",
			Size:   &size,
			URLs:   []string{"s3://bucket/key"},
			Hashes: map[string]string{"sha256": "sha"},
			Authz:  []string{"https://calypr.org/program/foo/project/bar"},
		}}})
	}))
	defer source.Close()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/index/bulk" {
			t.Fatalf("unexpected target path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&loaded); err != nil {
			t.Fatalf("decode target body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	sourceClient, err := NewHTTPClient(source.URL, AuthConfig{}, source.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient source: %v", err)
	}
	targetClient, err := NewHTTPClient(target.URL, AuthConfig{}, target.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient target: %v", err)
	}

	stats, err := Run(context.Background(), sourceClient, targetClient, Config{BatchSize: 100})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Loaded != 1 || len(loaded.Records) != 1 {
		t.Fatalf("expected one loaded record, stats=%+v body=%+v", stats, loaded)
	}
	if loaded.Records[0].Did != "dg.test/1" {
		t.Fatalf("unexpected loaded payload: %+v", loaded.Records[0])
	}
	if len(loaded.Records[0].ControlledAccess) != 1 || loaded.Records[0].ControlledAccess[0] != "/programs/foo/projects/bar" {
		t.Fatalf("expected controlled_access payload, got %+v", loaded.Records[0])
	}
	if len(loaded.Records[0].AccessMethods) != 1 || loaded.Records[0].AccessMethods[0].AccessUrl.Url != "s3://bucket/key" {
		t.Fatalf("expected access_methods payload, got %+v", loaded.Records[0])
	}
}

func TestHTTPClientRetriesTimeout(t *testing.T) {
	transport := &retryTransport{}
	client, err := NewHTTPClient("https://example.org/index", AuthConfig{}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}

	records, err := client.ListPage(context.Background(), 1, "")
	if err != nil {
		t.Fatalf("ListPage returned error: %v", err)
	}
	if transport.calls != 2 {
		t.Fatalf("expected one retry, got %d calls", transport.calls)
	}
	if len(records) != 1 || records[0].DID != "a" {
		t.Fatalf("unexpected records: %+v", records)
	}
}

func TestPreflightImportReportsAllMissingCreateScopes(t *testing.T) {
	reader := &fakeDumpReader{batches: [][]MigrationRecord{
		{
			{ID: "allowed", Checksums: []drs.Checksum{{Type: "sha256", Checksum: "sha-1"}}, Authz: []string{"/programs/cbds/projects/allowed"}},
			{ID: "denied-1", Checksums: []drs.Checksum{{Type: "sha256", Checksum: "sha-2"}}, Authz: []string{"/programs/cbds/projects/missing-one"}},
		},
		{
			{ID: "denied-2", Checksums: []drs.Checksum{{Type: "sha256", Checksum: "sha-3"}}, ControlledAccess: []string{"/programs/aced/projects/missing-two"}},
		},
	}}
	privileges := fakePrivilegeLister{privileges: map[string]map[string]bool{
		"/programs/cbds/projects/allowed": {"create": true},
	}}

	_, err := PreflightImport(context.Background(), reader, privileges, 500)
	if err == nil {
		t.Fatal("expected preflight to fail")
	}
	var preflightErr *ImportPreflightError
	if !errors.As(err, &preflightErr) {
		t.Fatalf("expected ImportPreflightError, got %T %v", err, err)
	}
	report := preflightErr.Report
	if report.Records != 3 || report.MissingRecords != 2 {
		t.Fatalf("unexpected report counts: %+v", report)
	}
	if report.FirstDeniedRecord != "denied-1" {
		t.Fatalf("unexpected first denied record: %q", report.FirstDeniedRecord)
	}
	body := err.Error()
	if !strings.Contains(body, "cbds/missing-one") || !strings.Contains(body, "aced/missing-two") {
		t.Fatalf("expected formatted missing scopes in error, got %q", body)
	}
}

func TestHTTPClientUserPrivileges(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user/user" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		resp := map[string]any{
			"authz": map[string]any{
				"/programs/cbds/projects/p1": []any{
					map[string]any{"service": "indexd", "method": "read"},
					map[string]any{"service": "drs", "method": "create"},
				},
				"/programs/cbds/projects/p2": []any{
					map[string]any{"service": "fence", "method": "create"},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL+"/index", AuthConfig{BearerToken: "tok"}, server.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	privileges, err := client.UserPrivileges(context.Background())
	if err != nil {
		t.Fatalf("UserPrivileges returned error: %v", err)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("expected bearer auth header, got %q", gotAuth)
	}
	if !privileges["/programs/cbds/projects/p1"]["create"] {
		t.Fatalf("expected create privilege for p1, got %+v", privileges)
	}
	if privileges["/programs/cbds/projects/p2"]["create"] {
		t.Fatalf("did not expect non-indexd/drs create privilege to be retained: %+v", privileges)
	}
}

type fakeSource struct {
	pages  [][]IndexdRecord
	starts []string
}

func (s *fakeSource) ListPage(ctx context.Context, limit int, start string) ([]IndexdRecord, error) {
	s.starts = append(s.starts, start)
	if len(s.starts) > len(s.pages) {
		return nil, nil
	}
	return s.pages[len(s.starts)-1], nil
}

type fakeLoader struct {
	records []MigrationRecord
}

func (l *fakeLoader) LoadBatch(ctx context.Context, records []MigrationRecord) error {
	l.records = append(l.records, records...)
	return nil
}

type fakeDumpReader struct {
	batches [][]MigrationRecord
}

func (r *fakeDumpReader) ReadBatches(ctx context.Context, batchSize int, fn func([]MigrationRecord) error) error {
	for _, batch := range r.batches {
		if err := fn(batch); err != nil {
			return err
		}
	}
	return nil
}

type fakePrivilegeLister struct {
	privileges map[string]map[string]bool
}

func (l fakePrivilegeLister) UserPrivileges(ctx context.Context) (map[string]map[string]bool, error) {
	return l.privileges, nil
}

type retryTransport struct {
	calls int
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls++
	if t.calls == 1 {
		return nil, timeoutErr{}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"records":[{"did":"a","size":1,"hashes":{"sha256":"sha-a"}}]}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
