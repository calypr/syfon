package migrate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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

func TestHTTPClientEndToEndWithMockIndexdAndSyfon(t *testing.T) {
	size := int64(7)
	var loaded struct {
		Records []struct {
			Did  string                         `json:"did"`
			Auth map[string]map[string][]string `json:"auth"`
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
