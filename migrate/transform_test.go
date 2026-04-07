package migrate

import (
	"testing"
	"time"
)

func TestTransform_BasicMapping(t *testing.T) {
	rec := IndexdRecord{
		DID:         "dg.TEST/abc-123",
		Size:        1024,
		FileName:    "sample.bam",
		Version:     "1",
		Description: "a test file",
		URLs:        []string{"s3://my-bucket/path/to/sample.bam"},
		Hashes:      map[string]string{"sha256": "deadbeef01234567", "md5": "cafebabe"},
		Authz:       []string{"/programs/open/projects/test"},
		CreatedDate: "2023-06-15T12:00:00.000000",
		UpdatedDate: "2023-06-16T08:30:00.000000",
		// Deprecated – must be silently dropped.
		Baseid:   "base-uuid",
		Rev:      "rev-abc",
		Uploader: "alice",
		ACL:      []string{"*"},
		Form:     "object",
	}

	obj, err := Transform(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Identity
	if obj.Id != "dg.TEST/abc-123" {
		t.Errorf("Id: got %q, want %q", obj.Id, "dg.TEST/abc-123")
	}
	if obj.SelfUri != "drs://dg.TEST/abc-123" {
		t.Errorf("SelfUri: got %q", obj.SelfUri)
	}

	// Core fields
	if obj.Size != 1024 {
		t.Errorf("Size: got %d, want 1024", obj.Size)
	}
	if obj.Name != "sample.bam" {
		t.Errorf("Name: got %q, want %q", obj.Name, "sample.bam")
	}
	if obj.Version != "1" {
		t.Errorf("Version: got %q, want %q", obj.Version, "1")
	}
	if obj.Description != "a test file" {
		t.Errorf("Description: got %q, want %q", obj.Description, "a test file")
	}

	// Timestamps must be parsed from indexd date strings.
	wantCreated, _ := time.Parse("2006-01-02T15:04:05.000000", "2023-06-15T12:00:00.000000")
	if !obj.CreatedTime.Equal(wantCreated.UTC()) {
		t.Errorf("CreatedTime: got %v, want %v", obj.CreatedTime, wantCreated.UTC())
	}
	wantUpdated, _ := time.Parse("2006-01-02T15:04:05.000000", "2023-06-16T08:30:00.000000")
	if !obj.UpdatedTime.Equal(wantUpdated.UTC()) {
		t.Errorf("UpdatedTime: got %v, want %v", obj.UpdatedTime, wantUpdated.UTC())
	}

	// Checksums
	if len(obj.Checksums) != 2 {
		t.Fatalf("Checksums: got %d, want 2", len(obj.Checksums))
	}
	csMap := make(map[string]string, len(obj.Checksums))
	for _, cs := range obj.Checksums {
		csMap[cs.Type] = cs.Checksum
	}
	if csMap["sha256"] != "deadbeef01234567" {
		t.Errorf("sha256 checksum: got %q", csMap["sha256"])
	}
	if csMap["md5"] != "cafebabe" {
		t.Errorf("md5 checksum: got %q", csMap["md5"])
	}

	// Access methods
	if len(obj.AccessMethods) != 1 {
		t.Fatalf("AccessMethods: got %d, want 1", len(obj.AccessMethods))
	}
	am := obj.AccessMethods[0]
	if am.AccessUrl.Url != "s3://my-bucket/path/to/sample.bam" {
		t.Errorf("AccessMethod URL: got %q", am.AccessUrl.Url)
	}
	if am.Type != "s3" {
		t.Errorf("AccessMethod Type: got %q, want %q", am.Type, "s3")
	}

	// Authz
	if len(obj.Authorizations) != 1 || obj.Authorizations[0] != "/programs/open/projects/test" {
		t.Errorf("Authorizations: got %v", obj.Authorizations)
	}
	// Authz must be propagated to the access method's bearer issuers.
	if len(am.Authorizations.BearerAuthIssuers) != 1 {
		t.Errorf("AccessMethod BearerAuthIssuers: got %v", am.Authorizations.BearerAuthIssuers)
	}
}

func TestTransform_EmptyDID(t *testing.T) {
	_, err := Transform(IndexdRecord{
		Hashes: map[string]string{"sha256": "abc"},
	})
	if err == nil {
		t.Fatal("expected error for empty DID")
	}
}

func TestTransform_FallbackTimestamps(t *testing.T) {
	rec := IndexdRecord{
		DID:    "test-uuid",
		Hashes: map[string]string{"sha256": "aabbcc"},
		// No dates – should fall back to now without panicking.
	}
	obj, err := Transform(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obj.CreatedTime.IsZero() {
		t.Error("CreatedTime should not be zero when not provided")
	}
	if obj.UpdatedTime.IsZero() {
		t.Error("UpdatedTime should not be zero when not provided")
	}
}

func TestTransform_RFC3339Date(t *testing.T) {
	rec := IndexdRecord{
		DID:         "did:example:rfc",
		Hashes:      map[string]string{"sha256": "ff"},
		CreatedDate: "2024-01-01T00:00:00Z",
	}
	obj, err := Transform(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if !obj.CreatedTime.Equal(want) {
		t.Errorf("CreatedTime: got %v, want %v", obj.CreatedTime, want)
	}
}

func TestTransform_MultipleURLs(t *testing.T) {
	rec := IndexdRecord{
		DID:    "multi-url",
		Hashes: map[string]string{"sha256": "abc"},
		URLs: []string{
			"s3://bucket/key",
			"gs://gcs-bucket/key",
			"https://example.org/file.bam",
			"",           // blank – must be skipped
			"ftp://host/path",
		},
	}
	obj, err := Transform(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(obj.AccessMethods) != 4 {
		t.Fatalf("AccessMethods: got %d, want 4", len(obj.AccessMethods))
	}
	expectTypes := []string{"s3", "gs", "https", "ftp"}
	for i, am := range obj.AccessMethods {
		if am.Type != expectTypes[i] {
			t.Errorf("AccessMethods[%d].Type: got %q, want %q", i, am.Type, expectTypes[i])
		}
	}
}

func TestTransform_DeduplicatesAuthz(t *testing.T) {
	rec := IndexdRecord{
		DID:    "dup-authz",
		Hashes: map[string]string{"sha256": "abc"},
		Authz:  []string{"/res/a", "/res/a", "/res/b"},
	}
	obj, err := Transform(rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(obj.Authorizations) != 2 {
		t.Errorf("expected deduplicated authz, got %v", obj.Authorizations)
	}
}

func TestTransformBatch(t *testing.T) {
	records := []IndexdRecord{
		{DID: "id-1", Hashes: map[string]string{"sha256": "aaa"}, Size: 100},
		{DID: "", Hashes: map[string]string{"sha256": "bbb"}}, // invalid – empty DID
		{DID: "id-3", Hashes: map[string]string{"sha256": "ccc"}, Size: 300},
	}

	objects, errs := TransformBatch(records)
	if len(objects) != 2 {
		t.Fatalf("expected 2 valid objects, got %d", len(objects))
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	if errs[0].DID != "" {
		t.Errorf("error DID: got %q, want %q", errs[0].DID, "")
	}
}

func TestInferAccessMethodType(t *testing.T) {
	cases := []struct {
		url  string
		want string
	}{
		{"s3://bucket/key", "s3"},
		{"gs://bucket/key", "gs"},
		{"az://container/blob", "az"},
		{"https://example.org/file", "https"},
		{"http://example.org/file", "http"},
		{"ftp://host/path", "ftp"},
		{"file:///local/path", "file"},
		{"", "https"},
		{"not-a-url", "https"},
	}
	for _, tc := range cases {
		got := inferAccessMethodType(tc.url)
		if got != tc.want {
			t.Errorf("inferAccessMethodType(%q) = %q, want %q", tc.url, got, tc.want)
		}
	}
}

func TestParseIndexdDate(t *testing.T) {
	cases := []struct {
		input string
		want  time.Time
	}{
		{"2023-06-15T12:00:00.000000", time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)},
		{"2023-06-15T12:00:00Z", time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)},
		{"2023-06-15", time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"", time.Time{}},
		{"not-a-date", time.Time{}},
	}
	for _, tc := range cases {
		got := parseIndexdDate(tc.input)
		if !got.Equal(tc.want) {
			t.Errorf("parseIndexdDate(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

