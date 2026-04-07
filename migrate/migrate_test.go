package migrate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// mockIndexdServer serves pages of IndexdRecords, optionally emitting a
// cursor (nextStart) to simulate cursor-based Indexd pagination.
// When useCursor is true each response includes a "start" field pointing at
// the next batch, and the final page returns an empty "start".
func mockIndexdServer(t *testing.T, records []IndexdRecord, useCursor bool) *httptest.Server {
	t.Helper()
	var call int32

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(atomic.AddInt32(&call, 1)) - 1

		q := r.URL.Query()
		limit := 100
		if v := q.Get("limit"); v != "" {
			_, _ = v, 0 // ignored for simplicity
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

		page := IndexdPage{Records: batch}
		if useCursor && end < len(records) {
			// emit a non-empty cursor so the client uses cursor mode
			page.Start = records[end].DID
		}
		// When end >= len(records): page.Start stays "" → signals end of stream.

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	}))
}

// mockSyfonServer accepts POST /index/migrate/bulk and records loaded DIDs.
func mockSyfonServer(t *testing.T, loaded *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/index/migrate/bulk" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var req struct {
			Records []struct {
				ID string `json:"id"`
			} `json:"records"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, rec := range req.Records {
			*loaded = append(*loaded, rec.ID)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"count": len(req.Records)})
	}))
}

func TestRun_BasicMigration(t *testing.T) {
	records := []IndexdRecord{
		{DID: "id-1", Size: 100, Hashes: map[string]string{"sha256": "aaa"}, Authz: []string{"/open"}},
		{DID: "id-2", Size: 200, Hashes: map[string]string{"sha256": "bbb"}, Authz: []string{"/open"}},
		{DID: "id-3", Size: 300, Hashes: map[string]string{"sha256": "ccc"}, Authz: []string{"/open"}},
	}

	indexdSrv := mockIndexdServer(t, records, false)
	defer indexdSrv.Close()

	var loaded []string
	syfonSrv := mockSyfonServer(t, &loaded)
	defer syfonSrv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  syfonSrv.URL,
		BatchSize: 10,
	})
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

func TestRun_CursorPagination_StopsOnEmptyCursor(t *testing.T) {
	// Build 25 records. With batch=10, we expect 3 fetches: 10, 10, 5.
	// The mock emits a cursor after batches 1 and 2; batch 3 returns empty cursor.
	// Bug #1 would keep looping; this test catches that.
	const total = 25
	records := make([]IndexdRecord, total)
	for i := range records {
		records[i] = IndexdRecord{
			DID:    "cursor-id-" + string(rune('A'+i)),
			Size:   int64(i),
			Hashes: map[string]string{"sha256": "hash"},
			Authz:  []string{"/open"},
		}
	}

	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(atomic.AddInt32(&callCount, 1)) - 1
		start := idx * 10
		end := start + 10
		if end > total {
			end = total
		}
		var batch []IndexdRecord
		if start < total {
			batch = records[start:end]
		}
		page := IndexdPage{Records: batch}
		if end < total {
			page.Start = records[end].DID // non-empty cursor
		}
		// Final batch: page.Start stays "" → cursor mode end of stream
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer srv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL: srv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		DryRun:    true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched != total {
		t.Errorf("Fetched: got %d, want %d", stats.Fetched, total)
	}
	calls := int(atomic.LoadInt32(&callCount))
	if calls != 3 {
		t.Errorf("source called %d times, want 3 (would loop forever with bug #1)", calls)
	}
}

func TestRun_HardLimitOnOversizedPage(t *testing.T) {
	// Source returns 20 records even though we requested 5 (limit=5).
	// Hard-limit behavior must cap processed records at 5.
	records := make([]IndexdRecord, 20)
	for i := range records {
		records[i] = IndexdRecord{
			DID:    "over-" + string(rune('A'+i)),
			Size:   10,
			Hashes: map[string]string{"sha256": "hh"},
			Authz:  []string{"/open"},
		}
	}

	srv := mockIndexdServer(t, records, false) // page mode, returns all 20 at once
	defer srv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL: srv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		Limit:     5,
		DryRun:    true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Fetched != 5 {
		t.Errorf("Fetched: got %d, want 5 (hard limit)", stats.Fetched)
	}
}

func TestRun_DryRun(t *testing.T) {
	records := []IndexdRecord{
		{DID: "id-dry", Size: 50, Hashes: map[string]string{"sha256": "fff"}, Authz: []string{"/open"}},
	}

	indexdSrv := mockIndexdServer(t, records, false)
	defer indexdSrv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		DryRun:    true,
	})
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
		{DID: "no-hash", Size: 10},
	}

	indexdSrv := mockIndexdServer(t, records, false)
	defer indexdSrv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL: indexdSrv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		DryRun:    true,
	})
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

func TestRun_LimitGatesNewFetches(t *testing.T) {
	// 20 records, limit=5, batch=10. Even if source ignores the requested limit,
	// processed records must still be capped at 5 and no second fetch should start.
	records := make([]IndexdRecord, 20)
	for i := range records {
		records[i] = IndexdRecord{
			DID:    "lim-" + string(rune('A'+i)),
			Size:   int64(i * 10),
			Hashes: map[string]string{"sha256": "hash"},
			Authz:  []string{"/open"},
		}
	}

	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(atomic.AddInt32(&callCount, 1)) - 1
		start := idx * 10
		end := start + 10
		if end > 20 {
			end = 20
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(IndexdPage{Records: records[start:end]})
	}))
	defer srv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL: srv.URL,
		SyfonURL:  "http://127.0.0.1:0",
		BatchSize: 10,
		Limit:     5,
		DryRun:    true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	calls := int(atomic.LoadInt32(&callCount))
	if calls > 1 {
		t.Errorf("source called %d times; limit should have prevented more than 1 fetch", calls)
	}
	if stats.Fetched != 5 {
		t.Errorf("Fetched: got %d, want 5 (hard limit)", stats.Fetched)
	}
}

func TestRun_DefaultAuthzApplied(t *testing.T) {
	records := []IndexdRecord{
		{DID: "no-authz", Size: 1, Hashes: map[string]string{"sha256": "abc"}},
	}

	indexdSrv := mockIndexdServer(t, records, false)
	defer indexdSrv.Close()

	var loaded []string
	syfonSrv := mockSyfonServer(t, &loaded)
	defer syfonSrv.Close()

	stats, err := Run(context.Background(), Config{
		IndexdURL:    indexdSrv.URL,
		SyfonURL:     syfonSrv.URL,
		BatchSize:    10,
		DefaultAuthz: []string{"/default/authz"},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if stats.Loaded != 1 {
		t.Errorf("Loaded: got %d, want 1", stats.Loaded)
	}
}

func TestRun_IdentityPreservedOnWire(t *testing.T) {
	// Verify that the wire record sent to /index/migrate/bulk carries the
	// full DRS-native metadata, not just hashes/URLs.
	var captured migrateWireRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/index/migrate/bulk" {
			_ = json.NewDecoder(r.Body).Decode(&captured)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 1})
			return
		}
		// source: single record page
		_ = json.NewEncoder(w).Encode(IndexdPage{Records: []IndexdRecord{{
			DID:         "dg.ORIG/abc-123",
			Size:        999,
			FileName:    "file.bam",
			Version:     "v2",
			Description: "my desc",
			Hashes:      map[string]string{"sha256": "deadbeef"},
			Authz:       []string{"/prog/proj"},
			CreatedDate: "2023-01-01T00:00:00Z",
		}}})
	}))
	defer srv.Close()

	_, err := Run(context.Background(), Config{
		IndexdURL: srv.URL,
		SyfonURL:  srv.URL, // same server handles both roles in this test
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(captured.Records) != 1 {
		t.Fatalf("captured %d wire records, want 1", len(captured.Records))
	}
	r := captured.Records[0]
	if r.ID != "dg.ORIG/abc-123" {
		t.Errorf("ID: got %q, want %q", r.ID, "dg.ORIG/abc-123")
	}
	if r.Name != "file.bam" {
		t.Errorf("Name: got %q", r.Name)
	}
	if r.Version != "v2" {
		t.Errorf("Version: got %q", r.Version)
	}
	if r.Description != "my desc" {
		t.Errorf("Description: got %q", r.Description)
	}
	if len(r.Checksums) == 0 || r.Checksums[0].Checksum != "deadbeef" {
		t.Errorf("Checksums: got %v", r.Checksums)
	}
	if len(r.Authz) == 0 || r.Authz[0] != "/prog/proj" {
		t.Errorf("Authz: got %v", r.Authz)
	}
	if r.CreatedTime.IsZero() {
		t.Error("CreatedTime must be set on wire record")
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		records IndexdRecord
		wantErr bool
	}{
		{name: "valid", records: IndexdRecord{DID: "id", Hashes: map[string]string{"sha256": "abc"}}},
		{name: "no checksums", records: IndexdRecord{DID: "id"}, wantErr: true},
		{name: "empty id", records: IndexdRecord{Hashes: map[string]string{"sha256": "abc"}}, wantErr: true},
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

