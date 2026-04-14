package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/syfon/client/xfer"
)

// GetDownloadResponse gets presigned URL and prepares HTTP response
func GetDownloadResponse(ctx context.Context, bk xfer.Downloader, fdr *downloadRequest, protocolText string) error {
	url, err := bk.ResolveDownloadURL(ctx, fdr.guid, protocolText)
	if err != nil {
		return fmt.Errorf("failed to resolve download URL for %s: %w", fdr.guid, err)
	}
	fdr.presignedURL = url

	return makeDownloadRequest(ctx, bk, fdr)
}

func makeDownloadRequest(ctx context.Context, bk xfer.Downloader, fdr *downloadRequest) error {
	resp, err := bk.Download(ctx, fdr.presignedURL, fdr.rangeStart, fdr.rangeEnd)

	if err != nil {
		return errors.New("Request failed: " + strings.ReplaceAll(err.Error(), fdr.presignedURL, "<SENSITIVE_URL>"))
	}

	// Check for non-success status codes
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close() // Ensure the body is closed

		bodyBytes, err := io.ReadAll(resp.Body)
		bodyString := "<unable to read body>"
		if err == nil {
			bodyString = string(bodyBytes)
		}

		return fmt.Errorf("non-OK response: %d, body: %s", resp.StatusCode, bodyString)
	}

	fdr.response = resp
	return nil
}
