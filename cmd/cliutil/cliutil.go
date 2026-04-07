package cliutil

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	syclient "github.com/calypr/syfon/client"
	"github.com/spf13/cobra"
)

func NormalizedServerURL(cmd *cobra.Command) string {
	base, err := cmd.Flags().GetString("server")
	if err != nil {
		base = ""
	}
	base = strings.TrimSpace(base)
	if base == "" {
		base = "http://127.0.0.1:8080"
	}
	return strings.TrimRight(base, "/")
}

func NewHTTPClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second}
}

func NewSyfonClient(cmd *cobra.Command) *syclient.Client {
	return syclient.New(NormalizedServerURL(cmd), syclient.WithHTTPClient(NewHTTPClient()))
}



func GetInternalRecord(ctx context.Context, c *syclient.Client, did string) (*syclient.InternalRecord, error) {
	out, err := c.GetRecord(ctx, did)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func PutInternalRecord(ctx context.Context, c *syclient.Client, did string, rec *syclient.InternalRecord) error {
	return c.PutRecord(ctx, did, *rec)
}

func PostInternalRecord(ctx context.Context, c *syclient.Client, rec *syclient.InternalRecord) error {
	return c.PostRecord(ctx, *rec)
}

func CanonicalObjectURLFromSignedURL(signedURL, bucketHint, fallbackDID string) (string, error) {
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

func EnsureRecordWithURL(ctx context.Context, c *syclient.Client, did, objectURL, fileName string, size int64, sha256sum string) error {
	existing, err := GetInternalRecord(ctx, c, did)
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
		return PutInternalRecord(ctx, c, did, existing)
	}

	payload := syclient.InternalRecord{}
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
	return PostInternalRecord(ctx, c, &payload)
}
