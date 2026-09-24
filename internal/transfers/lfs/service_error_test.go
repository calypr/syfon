package lfs

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestSanitizeSignedPartRequestErrorRedactsNestedURLAndPreservesCause(t *testing.T) {
	const signedURL = "https://part-user:part-password@parts.example/upload?X-Amz-Signature=synthetic-part-secret#synthetic-part-fragment"
	sentinel := errors.New("connection refused")
	transportErr := fmt.Errorf("wrapped transport failure: %w", &url.Error{
		Op:  http.MethodPut,
		URL: signedURL,
		Err: sentinel,
	})

	err := sanitizeSignedPartRequestError(transportErr, signedURL)
	if !errors.Is(err, sentinel) {
		t.Fatalf("sanitized error lost transport cause: %v", err)
	}
	for _, secret := range []string{"part-user", "part-password", "X-Amz-Signature", "synthetic-part-secret", "synthetic-part-fragment"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("sanitized error leaked %q: %v", secret, err)
		}
	}
	if !strings.Contains(err.Error(), "https://parts.example/upload") {
		t.Fatalf("sanitized error omitted useful URL context: %v", err)
	}
	var requestErr *url.Error
	if !errors.As(err, &requestErr) {
		t.Fatalf("sanitized error lost url.Error context: %v", err)
	}
	if requestErr.URL != "https://parts.example/upload" {
		t.Fatalf("sanitized url.Error URL = %q", requestErr.URL)
	}
}
