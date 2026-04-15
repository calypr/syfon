package xfer

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/calypr/syfon/client/pkg/common"
)

// SignedURLBackend implements RangeReader using server-signed URLs.
type SignedURLBackend struct {
	signer PartSigner
}

func NewSignedURLBackend(signer PartSigner) RangeReader {
	return &SignedURLBackend{signer: signer}
}

func (b *SignedURLBackend) Stat(ctx context.Context, guid string) (*ObjectMetadata, error) {
	return &ObjectMetadata{
		AcceptRanges: true,
		Provider:     "syfon-signed-url",
	}, nil
}

func (b *SignedURLBackend) GetReader(ctx context.Context, guid string) (io.ReadCloser, error) {
	return b.GetRangeReader(ctx, guid, 0, -1)
}

func (b *SignedURLBackend) GetRangeReader(ctx context.Context, guid string, offset, length int64) (io.ReadCloser, error) {
	end := offset + length - 1
	if length <= 0 {
		end = -1
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		bodyErr := common.ResponseBodyError(resp, "signed url request failed")
		resp.Body.Close()
		return nil, bodyErr
	}

	return resp.Body, nil
}
