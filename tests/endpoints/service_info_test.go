package endpoints

import (
	"encoding/json"
	"net/http"
	"testing"
)

type serviceInfoResponse struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

func TestServiceInfoFromMainBinary(t *testing.T) {
	resp, err := http.Get(baseURL + "/ga4gh/drs/v1/service-info")
	if err != nil {
		t.Fatalf("failed to GET /service-info: %v\nstdout:\n%s\nstderr:\n%s",
			err, testStdout.String(), testStderr.String())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d\nstdout:\n%s\nstderr:\n%s",
			http.StatusOK, resp.StatusCode, testStdout.String(), testStderr.String())
	}

	var body serviceInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON: %v\nstdout:\n%s\nstderr:\n%s",
			err, testStdout.String(), testStderr.String())
	}

	if body.Name == "" {
		t.Fatalf("expected non-empty name, got empty\nstdout:\n%s\nstderr:\n%s",
			testStdout.String(), testStderr.String())
	}
	if body.CreatedAt == "" {
		t.Fatalf("expected non-empty created_at, got empty\nstdout:\n%s\nstderr:\n%s",
			testStdout.String(), testStderr.String())
	}
}
