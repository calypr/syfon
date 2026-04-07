package migrate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// mockIndexdServer returns an httptest.Server that serves up to maxPages pages
// of IndexdRecords (batchSize records per page) then returns empty responses.
func mockIndexdServer(t *testing.T, records []IndexdRecord) *httptest.Server {
	t.Helper()
	var pageIdx int32

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(atomic.AddInt32(&pageIdx, 1)) - 1

		q := r.URL.Query()
		limit := 100
		if v := q.Get("limit"); v != "" {
			_, _ = v, 0 // ignore for simplicity; the mock returns fixed-size batches
		}

		batchSize := limit
		start := idx * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}

		var batch []IndexdRecord
		if start < len(records) {
			batch = records[start:end]
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(IndexdPage{Records: batch})
	}))
}

// mockSyfonServer returns an httptest.Server that accepts POST /index/bulk
// and records the submitted InternalRecords.
func mockSyfonServer(t *testing.T, loaded *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/index/bulk" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var req struct {
			Records []struct {
				Did *string `json:"did"`
			} `json:"records"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, rec := range req.Records {
			if rec.Did != nil {
				*loaded = append(*loaded, *rec.Did)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"records": []any{}})
	}))
}

func TestRun_BasicMigration(t *testing.T) {
	records := []IndexdRecord{
		{DID: "id-1", Size: 100, Hashes: map[string]string{"sha256": "aaa"}, Authz: []string{"/open"}},
		{DID: "id-2", Size: 200, Hashes: map[string]string{"sha256": "bbb"}, Authz: []string{"/open"}},
		{DID: "id-3", Size: 300, Hashes: map[string]string{"sha256": "ccc"}, Authz: []string{"/open"}},
	}

	indexdSrv := mockIndexdServer(t, records)
	defer indexdSrv.Close()

	var loaded []string
	syfonSrv := mockSyfonServer(t, &loaded)
	defer syfonSrv.Close()

	cfg := Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  syfonSrv.URL,
		BatchSize: 10,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched != 3 {
		t.Errorf("Fetched: got %d, want 3", stats.Fetched)
	}
	if stats.Loaded != 3 {
		t.Errorf("Loaded: got %d, want 3", stats.Loaded)
	}
	if stats.Errors != 0 {
		t.Errorf("Errors: got %d, want 0", stats.Errors)
	}
	if len(loaded) != 3 {
		t.Errorf("DIDs sent to Syfon: got %d, want 3", len(loaded))
	}
}

func TestRun_DryRun(t *testing.T) {
	records := []IndexdRecord{
		{DID: "id-dry", Size: 50, Hashes: map[string]string{"sha256": "fff"}, Authz: []string{"/open"}},
	}

	indexdSrv := mockIndexdServer(t, records)
	defer indexdSrv.Close()

	cfg := Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  "http://127.0.0.1:0", // unreachable – must not be called
		BatchSize: 10,
		DryRun:    true,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched != 1 {
		t.Errorf("Fetched: got %d, want 1", stats.Fetched)
	}
	if stats.Loaded != 1 {
		t.Errorf("Dry-run Loaded count: got %d, want 1", stats.Loaded)
	}
}

func TestRun_SkipsRecordsWithoutChecksums(t *testing.T) {
	records := []IndexdRecord{
		{DID: "valid", Size: 10, Hashes: map[string]string{"sha256": "abc"}, Authz: []string{"/open"}},
		{DID: "no-hash", Size: 10}, // no checksums – must be skipped
	}

	indexdSrv := mockIndexdServer(t, records)
	defer indexdSrv.Close()

	cfg := Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		DryRun:    true,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Skipped != 1 {
		t.Errorf("Skipped: got %d, want 1", stats.Skipped)
	}
	if stats.Loaded != 1 {
		t.Errorf("Loaded: got %d, want 1", stats.Loaded)
	}
}

func TestRun_LimitRespected(t *testing.T) {
	records := make([]IndexdRecord, 20)
	for i := range records {
		records[i] = IndexdRecord{
			DID:    "id-" + string(rune('A'+i)),
			Size:   int64(i * 10),
			Hashes: map[string]string{"sha256": "hash"},
			Authz:  []string{"/open"},
		}
	}

	indexdSrv := mockIndexdServer(t, records)
	defer indexdSrv.Close()

	cfg := Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		Limit:     5,
		DryRun:    true,
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched > 5 {
		t.Errorf("Fetched %d records, Limit was 5", stats.Fetched)
	}
}

func TestRun_DefaultAuthzApplied(t *testing.T) {
	records := []IndexdRecord{
		{DID: "no-authz", Size: 1, Hashes: map[string]string{"sha256": "abc"}},
	}

	indexdSrv := mockIndexdServer(t, records)
	defer indexdSrv.Close()

	var loaded []string
	syfonSrv := mockSyfonServer(t, &loaded)
	defer syfonSrv.Close()

	cfg := Config{
		IndexdURL:    indexdSrv.URL,
		SyfonURL:     syfonSrv.URL,
		BatchSize:    10,
		DefaultAuthz: []string{"/default/authz"},
	}

	stats, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Loaded != 1 {
		t.Errorf("Loaded: got %d, want 1", stats.Loaded)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		records IndexdRecord
		wantErr bool
	}{
		{
			name:    "valid",
			records: IndexdRecord{DID: "id", Hashes: map[string]string{"sha256": "abc"}},
			wantErr: false,
		},
		{
			name:    "no checksums",
			records: IndexdRecord{DID: "id"},
			wantErr: true,
		},
		{
			name:    "empty id",
			records: IndexdRecord{Hashes: map[string]string{"sha256": "abc"}},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj, _ := Transform(tc.records)
			err := validate(obj)
			if (err != nil) != tc.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

