package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/transfer"
)

// GetDownloadResponse gets presigned URL and prepares HTTP response
func GetDownloadResponse(ctx context.Context, bk transfer.Downloader, fdr *common.FileDownloadResponseObject, protocolText string) error {
	url, err := bk.ResolveDownloadURL(ctx, fdr.GUID, protocolText)
	if err != nil {
		return fmt.Errorf("failed to resolve download URL for %s: %w", fdr.GUID, err)
	}
	fdr.PresignedURL = url

	return makeDownloadRequest(ctx, bk, fdr)
}

func makeDownloadRequest(ctx context.Context, bk transfer.Downloader, fdr *common.FileDownloadResponseObject) error {
	resp, err := bk.Download(ctx, fdr)

	if err != nil {
		return errors.New("Request failed: " + strings.ReplaceAll(err.Error(), fdr.PresignedURL, "<SENSITIVE_URL>"))
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

	fdr.Response = resp
	return nil
}
