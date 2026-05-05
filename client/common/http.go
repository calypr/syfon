package common

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ResponseBodyError formats a non-2xx HTTP response with a bounded body preview.
func ResponseBodyError(resp *http.Response, prefix string) error {
	if resp == nil {
		return fmt.Errorf("%s: nil response", prefix)
	}

	const maxBodyPreview = 4 << 10
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyPreview))
	if err != nil {
		return fmt.Errorf("%s: status %d body-read-error=%v", prefix, resp.StatusCode, err)
	}
	body := strings.TrimSpace(string(bodyBytes))
	if body == "" {
		return fmt.Errorf("%s: status %d", prefix, resp.StatusCode)
	}
	return fmt.Errorf("%s: status %d body=%s", prefix, resp.StatusCode, body)
}
