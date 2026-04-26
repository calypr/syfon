package common

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestToJSONReader(t *testing.T) {
	t.Parallel()

	reader, err := ToJSONReader(map[string]string{"hello": "world"})
	if err != nil {
		t.Fatalf("ToJSONReader returned error: %v", err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if got := strings.TrimSpace(string(body)); got != `{"hello":"world"}` {
		t.Fatalf("unexpected JSON body: %s", got)
	}
}

func TestParseRootPathAndGetAbsolutePath(t *testing.T) {
	t.Parallel()

	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir returned error: %v", err)
	}

	expanded, err := ParseRootPath("~/syfon-test")
	if err != nil {
		t.Fatalf("ParseRootPath returned error: %v", err)
	}
	if expanded != filepath.Join(home, "syfon-test") {
		t.Fatalf("unexpected expanded path: %q", expanded)
	}

	abs, err := GetAbsolutePath(".")
	if err != nil {
		t.Fatalf("GetAbsolutePath returned error: %v", err)
	}
	if !filepath.IsAbs(abs) {
		t.Fatalf("expected absolute path, got %q", abs)
	}
}

func TestParseFilePaths(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	visible := filepath.Join(tempDir, "visible.txt")
	metadata := filepath.Join(tempDir, "visible_metadata.json")
	hidden := filepath.Join(tempDir, ".hidden.txt")
	nestedDir := filepath.Join(tempDir, "nested")
	nestedVisible := filepath.Join(nestedDir, "nested.txt")
	nestedMetadata := filepath.Join(nestedDir, "nested_metadata.json")
	nestedHidden := filepath.Join(nestedDir, "~skip.txt")

	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	for _, path := range []string{visible, metadata, hidden, nestedVisible, nestedMetadata, nestedHidden} {
		if err := os.WriteFile(path, []byte("ok"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) returned error: %v", path, err)
		}
	}

	withMetadata, err := ParseFilePaths(filepath.Join(tempDir, "*"), false)
	if err != nil {
		t.Fatalf("ParseFilePaths without metadata filtering returned error: %v", err)
	}
	gotWithout := toPathSet(withMetadata)
	if !gotWithout[visible] || !gotWithout[metadata] || !gotWithout[nestedVisible] || !gotWithout[nestedMetadata] {
		t.Fatalf("expected visible, metadata, and nested files; got %+v", withMetadata)
	}
	if gotWithout[hidden] || gotWithout[nestedHidden] {
		t.Fatalf("did not expect hidden files in result: %+v", withMetadata)
	}

	withoutMetadata, err := ParseFilePaths(filepath.Join(tempDir, "*"), true)
	if err != nil {
		t.Fatalf("ParseFilePaths with metadata filtering returned error: %v", err)
	}
	gotWith := toPathSet(withoutMetadata)
	if !gotWith[metadata] {
		t.Fatalf("expected root metadata file to remain in result: %+v", withoutMetadata)
	}
	if gotWith[nestedMetadata] {
		t.Fatalf("did not expect nested metadata file in result: %+v", withoutMetadata)
	}
	if !gotWith[visible] || !gotWith[nestedVisible] {
		t.Fatalf("expected visible and nested files after filtering; got %+v", withoutMetadata)
	}
}

func TestResponseBodyError(t *testing.T) {
	t.Parallel()

	if err := ResponseBodyError(nil, "fetch failed"); err == nil || err.Error() != "fetch failed: nil response" {
		t.Fatalf("unexpected nil-response error: %v", err)
	}

	err := ResponseBodyError(&http.Response{
		StatusCode: http.StatusForbidden,
		Body:       io.NopCloser(strings.NewReader("   denied   ")),
	}, "fetch failed")
	if err == nil || err.Error() != "fetch failed: status 403 body=denied" {
		t.Fatalf("unexpected body error: %v", err)
	}

	err = ResponseBodyError(&http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       io.NopCloser(strings.NewReader("   ")),
	}, "fetch failed")
	if err == nil || err.Error() != "fetch failed: status 502" {
		t.Fatalf("unexpected empty-body error: %v", err)
	}
}

func TestCanDownloadFile(t *testing.T) {
	t.Parallel()

	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=0-0" {
			t.Fatalf("unexpected range header: %q", got)
		}
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer okServer.Close()

	if err := CanDownloadFile(okServer.URL); err != nil {
		t.Fatalf("CanDownloadFile returned error for 206 response: %v", err)
	}

	forbiddenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "access denied", http.StatusForbidden)
	}))
	defer forbiddenServer.Close()

	err := CanDownloadFile(forbiddenServer.URL)
	if err == nil || !strings.Contains(err.Error(), "failed to access file: status 403 body=access denied") {
		t.Fatalf("unexpected download error: %v", err)
	}
}

func TestIsCloudPresignedURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want bool
	}{
		{name: "aws v4", url: "https://example.test?X-Amz-Signature=abc", want: true},
		{name: "gcs", url: "https://example.test?X-Goog-Signature=abc", want: true},
		{name: "legacy", url: "https://example.test?AWSAccessKeyId=abc&Expires=1", want: true},
		{name: "plain", url: "https://example.test/file.txt", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCloudPresignedURL(tc.url); got != tc.want {
				t.Fatalf("IsCloudPresignedURL(%q) = %v, want %v", tc.url, got, tc.want)
			}
		})
	}
}

func TestLoadFailedLog(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "failed.json")
	want := map[string]RetryObject{
		"one": {
			SourcePath: "/tmp/file1",
			GUID:       "guid-1",
			RetryCount: 2,
			Bucket:     "bucket-1",
			FileMetadata: FileMetadata{
				Authorizations: map[string][]string{"test": {}},
				Aliases:        []string{"alias-1"},
				Metadata:       map[string]any{"k": "v"},
			},
		},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	got, err := LoadFailedLog(path)
	if err != nil {
		t.Fatalf("LoadFailedLog returned error: %v", err)
	}
	if got["one"].SourcePath != want["one"].SourcePath || got["one"].GUID != want["one"].GUID || got["one"].Bucket != want["one"].Bucket {
		t.Fatalf("unexpected retry object: %+v", got["one"])
	}
	if got["one"].FileMetadata.Metadata["k"] != "v" {
		t.Fatalf("unexpected metadata: %+v", got["one"].FileMetadata.Metadata)
	}
}

func TestProgressContextHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	if GetProgress(ctx) != nil {
		t.Fatal("expected nil progress callback from bare context")
	}
	if got := GetOid(ctx); got != "" {
		t.Fatalf("expected empty oid from bare context, got %q", got)
	}

	var seen ProgressEvent
	cb := func(evt ProgressEvent) error {
		seen = evt
		return nil
	}
	ctx = WithProgress(ctx, cb)
	ctx = WithOid(ctx, "oid-123")

	gotCB := GetProgress(ctx)
	if gotCB == nil {
		t.Fatal("expected progress callback in context")
	}
	if err := gotCB(ProgressEvent{Event: "tick", Oid: "oid-123"}); err != nil {
		t.Fatalf("callback returned error: %v", err)
	}
	if seen.Event != "tick" || seen.Oid != "oid-123" {
		t.Fatalf("unexpected progress event: %+v", seen)
	}
	if got := GetOid(ctx); got != "oid-123" {
		t.Fatalf("unexpected oid: %q", got)
	}
}

func TestCleanupHiddenFiles(t *testing.T) {
	t.Parallel()

	visible := filepath.Join(t.TempDir(), "visible.txt")
	hidden := filepath.Join(filepath.Dir(visible), ".hidden.txt")
	paths := cleanupHiddenFiles([]string{visible, hidden})
	if len(paths) != 1 || paths[0] != visible {
		t.Fatalf("unexpected filtered paths: %+v", paths)
	}
}

func TestLoadFailedLogMissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadFailedLog(filepath.Join(t.TempDir(), "missing.json"))
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}

func toPathSet(paths []string) map[string]bool {
	result := make(map[string]bool, len(paths))
	for _, path := range paths {
		result[path] = true
	}
	return result
}
