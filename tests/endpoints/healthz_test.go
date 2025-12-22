package endpoints

import (
	"net/http"
	"testing"
)

func TestHealthzFromMainBinary(t *testing.T) {
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("failed to GET /healthz: %v\nstdout:\n%s\nstderr:\n%s",
			err, testStdout.String(), testStderr.String())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d\nstdout:\n%s\nstderr:\n%s",
			http.StatusOK, resp.StatusCode, testStdout.String(), testStderr.String())
	}
}
