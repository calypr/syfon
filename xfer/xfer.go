package xfer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func httpClientOrDefault(hc *http.Client) *http.Client {
	if hc != nil {
		return hc
	}
	return http.DefaultClient
}

// Upload performs a PUT against a signed URL and returns ETag (when provided).
// It supports http(s) and file:// URLs.
func Upload(ctx context.Context, hc *http.Client, signedURL string, body io.Reader, size int64) (string, error) {
	parsed, err := url.Parse(signedURL)
	if err != nil {
		return "", fmt.Errorf("parse signed url: %w", err)
	}

	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, body)
		if err != nil {
			return "", err
		}
		if size > 0 {
			req.ContentLength = size
		}
		resp, err := httpClientOrDefault(hc).Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			return "", fmt.Errorf("upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
		return strings.Trim(resp.Header.Get("ETag"), `"`), nil
	case "file":
		target := parsed.Path
		if target == "" {
			return "", fmt.Errorf("file signed URL has empty path")
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return "", err
		}
		f, err := os.Create(target)
		if err != nil {
			return "", err
		}
		defer f.Close()
		if _, err := io.Copy(f, body); err != nil {
			return "", err
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported upload URL scheme: %s", parsed.Scheme)
	}
}

func UploadBytes(ctx context.Context, hc *http.Client, signedURL string, payload []byte) error {
	_, err := Upload(ctx, hc, signedURL, bytes.NewReader(payload), int64(len(payload)))
	return err
}

// DownloadToPath fetches a signed URL to a local file path.
// It supports http(s) and file:// URLs.
func DownloadToPath(ctx context.Context, hc *http.Client, signedURL, outPath string) error {
	parsed, err := url.Parse(signedURL)
	if err != nil {
		return fmt.Errorf("parse signed url: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL, nil)
		if err != nil {
			return err
		}
		resp, err := httpClientOrDefault(hc).Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("download failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
		f, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, resp.Body)
		return err
	case "file":
		srcPath := parsed.Path
		src, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	default:
		return fmt.Errorf("unsupported download URL scheme: %s", parsed.Scheme)
	}
}
