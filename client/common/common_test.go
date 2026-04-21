package common

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestToJSONReader(t *testing.T) {
	payload := map[string]string{"foo": "bar"}
	reader, err := ToJSONReader(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var decoded map[string]string
	json.NewDecoder(reader).Decode(&decoded)
	if decoded["foo"] != "bar" {
		t.Errorf("expected bar, got %s", decoded["foo"])
	}
}

func TestParseRootPath(t *testing.T) {
	t.Run("NormalPath", func(t *testing.T) {
		p, err := ParseRootPath("/tmp/test")
		if err != nil || p != "/tmp/test" {
			t.Errorf("unexpected: %s, %v", p, err)
		}
	})

	t.Run("HomePath", func(t *testing.T) {
		home, _ := os.UserHomeDir()
		p, err := ParseRootPath("~/test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.HasPrefix(p, home) {
			t.Errorf("expected path to start with %s, got %s", home, p)
		}
	})
}

func TestIsCloudPresignedURL(t *testing.T) {
	cases := []struct {
		url      string
		expected bool
	}{
		{"https://s3.amazonaws.com/b/k?X-Amz-Signature=123", true},
		{"https://storage.googleapis.com/b/k?X-Goog-Signature=123", true},
		{"https://example.com/file?Signature=123", true},
		{"https://example.com/file", false},
	}
	for _, tc := range cases {
		if got := IsCloudPresignedURL(tc.url); got != tc.expected {
			t.Errorf("IsCloudPresignedURL(%s) = %v, expected %v", tc.url, got, tc.expected)
		}
	}
}

func TestCanDownloadFile(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Range") != "bytes=0-0" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
		}))
		defer server.Close()

		err := CanDownloadFile(server.URL)
		if err != nil {
			t.Errorf("expected success, got %v", err)
		}
	})

	t.Run("Failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"message": "access denied"}`))
		}))
		defer server.Close()

		err := CanDownloadFile(server.URL)
		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}

// Minimal test for LoadFailedLog
func TestLoadFailedLog(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "failed_log_*.json")
	defer os.Remove(tmpFile.Name())

	content := `{"obj1": {"guid": "123", "retrycount": 2}}`
	os.WriteFile(tmpFile.Name(), []byte(content), 0644)

	m, err := LoadFailedLog(tmpFile.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m) != 1 || m["obj1"].GUID != "123" {
		t.Errorf("unexpected content: %+v", m)
	}
}
