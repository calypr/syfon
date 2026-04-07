package transfer

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// SignedURLBackend implements RangeReader using server-signed URLs.
// This allows machines without local cloud credentials to perform parallel downloads.
type SignedURLBackend struct {
	signer PartSigner
}

func NewSignedURLBackend(signer PartSigner) RangeReader {
	return &SignedURLBackend{signer: signer}
}

func (b *SignedURLBackend) Stat(ctx context.Context, guid string) (*ObjectMetadata, error) {
	// SignedURLBackend relies on the Resolver to have already performed Stat if needed.
	// However, we can return a basic metadata object.
	return &ObjectMetadata{
		AcceptRanges: true,
		Provider:     "syfon-signed-url",
	}, nil
}

func (b *SignedURLBackend) GetReader(ctx context.Context, guid string) (io.ReadCloser, error) {
	// For a single stream, we just sign a large range (or the whole thing).
	// But usually, GetRangeReader is preferred for this backend.
	return b.GetRangeReader(ctx, guid, 0, -1)
}

func (b *SignedURLBackend) GetRangeReader(ctx context.Context, guid string, offset, length int64) (io.ReadCloser, error) {
	end := offset + length - 1
	if length <= 0 {
		end = -1 // Let the server decide or sign for the whole object
	}

	signedURL, err := b.signer.GetDownloadPartURL(ctx, guid, offset, end)
	if err != nil {
		return nil, fmt.Errorf("failed to sign range %d-%d: %w", offset, end, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL.URL, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range signedURL.Headers {
		req.Header.Set(k, v)
	}

	// Important: We do NOT add the Range header here because the URL is already 
	// cryptographically bound to the range by the server (especially for GCS V4).
	// If the server didn't bake the range into the signature (e.g. S3), adding it here 
	// is redundant but safe. If it IS GCS V4, adding it here might cause a mismatch 
	// if the server used a slightly different format.
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("signed url request failed (%d): %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}
