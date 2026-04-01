package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/internalapi"
)

var Version = "dev"

func normalizedServerURL() string {
	base := strings.TrimSpace(serverBaseURL)
	if base == "" {
		return "http://127.0.0.1:8080"
	}
	return strings.TrimRight(base, "/")
}

func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second}
}

func doJSON(method, path string, body any, out any) error {
	base := normalizedServerURL()
	fullURL := base + path

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, fullURL, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := newHTTPClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s failed: status=%d body=%s", method, fullURL, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func uploadBytesToSignedURL(signedURL string, payload []byte) error {
	parsed, err := url.Parse(signedURL)
	if err != nil {
		return fmt.Errorf("parse signed url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		req, err := http.NewRequest(http.MethodPut, signedURL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		resp, err := newHTTPClient().Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
		return nil
	case "file":
		target := parsed.Path
		if target == "" {
			return fmt.Errorf("file signed URL has empty path")
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, payload, 0o644)
	default:
		return fmt.Errorf("unsupported upload URL scheme: %s", parsed.Scheme)
	}
}

func downloadSignedURLToPath(signedURL, outPath string) error {
	parsed, err := url.Parse(signedURL)
	if err != nil {
		return fmt.Errorf("parse signed url: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		resp, err := newHTTPClient().Get(signedURL)
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

func getInternalRecord(did string) (*internalapi.InternalRecord, error) {
	var out internalapi.InternalRecord
	if err := doJSON(http.MethodGet, "/index/"+url.PathEscape(did), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func putInternalRecord(did string, rec *internalapi.InternalRecord) error {
	return doJSON(http.MethodPut, "/index/"+url.PathEscape(did), rec, nil)
}

func postInternalRecord(rec *internalapi.InternalRecord) error {
	return doJSON(http.MethodPost, "/index", rec, nil)
}

func canonicalObjectURLFromSignedURL(signedURL, bucketHint, fallbackDID string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(signedURL))
	if err != nil {
		return "", fmt.Errorf("parse signed url: %w", err)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""

	switch strings.ToLower(parsed.Scheme) {
	case "file":
		return parsed.String(), nil
	case "http", "https":
		bucketHint = strings.TrimSpace(bucketHint)
		if bucketHint == "" {
			return "", fmt.Errorf("server returned upload URL without bucket; cannot canonicalize object URL safely")
		}
		key := strings.Trim(strings.TrimSpace(parsed.Path), "/")
		if strings.HasPrefix(key, bucketHint+"/") {
			key = strings.TrimPrefix(key, bucketHint+"/")
		}
		if key == "" {
			key = strings.TrimSpace(fallbackDID)
		}
		if key == "" {
			return "", fmt.Errorf("unable to derive object key from upload URL")
		}
		return "s3://" + bucketHint + "/" + key, nil
	default:
		return parsed.String(), nil
	}
}

func ensureRecordWithURL(did, objectURL, fileName string, size int64, sha256sum string) error {
	existing, err := getInternalRecord(did)
	if err == nil {
		if strings.TrimSpace(existing.GetDid()) == "" {
			existing.SetDid(did)
		}
		if fileName != "" {
			existing.SetFileName(fileName)
		}
		if size > 0 {
			existing.SetSize(size)
		}
		if objectURL != "" {
			urls := append([]string(nil), existing.GetUrls()...)
			seen := map[string]bool{}
			for _, u := range urls {
				seen[u] = true
			}
			if !seen[objectURL] {
				urls = append(urls, objectURL)
				existing.SetUrls(urls)
			}
		}
		if sha256sum != "" {
			hashes := existing.GetHashes()
			if hashes == nil {
				hashes = map[string]string{}
			}
			hashes["sha256"] = sha256sum
			existing.SetHashes(hashes)
		}
		return putInternalRecord(did, existing)
	}

	payload := internalapi.NewInternalRecord()
	payload.SetDid(did)
	if size > 0 {
		payload.SetSize(size)
	}
	if objectURL != "" {
		payload.SetUrls([]string{objectURL})
	}
	if fileName != "" {
		payload.SetFileName(fileName)
	}
	if sha256sum != "" {
		payload.SetHashes(map[string]string{"sha256": sha256sum})
	}
	return postInternalRecord(payload)
}
