package services

import (
	"fmt"
	"net/url"
	"strings"
)

func (d *DataService) CanonicalObjectURL(signedURL, bucketHint, fallbackDID string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(signedURL))
	if err != nil {
		return "", fmt.Errorf("parse signed url: %w", err)
	}
	originalParsed := *parsed
	parsed.RawQuery = ""
	parsed.Fragment = ""

	switch strings.ToLower(parsed.Scheme) {
	case "file":
		return parsed.String(), nil
	case "http", "https":
		if b, k, ok := parseGCSJSONUploadURL(&originalParsed); ok {
			return "s3://" + b + "/" + k, nil
		}
		if b, k, ok := parseAzureBlobSignedURL(&originalParsed); ok {
			return "s3://" + b + "/" + k, nil
		}

		bucketHint = strings.TrimSpace(bucketHint)

		key := strings.Trim(strings.TrimSpace(parsed.Path), "/")

		// If bucketHint is empty, try to infer it from the first segment of the path (Path-Style)
		if bucketHint == "" {
			parts := strings.Split(key, "/")
			if len(parts) > 1 {
				bucketHint = parts[0]
				key = strings.Join(parts[1:], "/")
			}
		}

		if bucketHint == "" {
			return "", fmt.Errorf("unable to determine bucket context from URL: %s", signedURL)
		}

		// If the path starts with /bucket/, strip it to get the key.
		if strings.HasPrefix(key, bucketHint+"/") {
			key = strings.TrimPrefix(key, bucketHint+"/")
		}

		// Use s3:// as the standard internal representation for all HTTP-signed cloud storage (MinIO/S3/GCS)
		// unless we have specific knowledge to do otherwise.
		if key == "" {
			key = strings.TrimSpace(fallbackDID)
		}
		if key == "" {
			return "", fmt.Errorf("unable to derive object key from upload URL")
		}
		return "s3://" + bucketHint + "/" + key, nil
	default:
		if parsed.Scheme != "" && parsed.Host != "" {
			return parsed.String(), nil
		}
		key := strings.TrimSpace(fallbackDID)
		if key == "" && parsed.Path != "" {
			parts := strings.Split(parsed.Path, "/")
			if last := parts[len(parts)-1]; last != "" {
				key = last
			}
		}
		if key == "" {
			return "", fmt.Errorf("unable to derive object key from upload URL")
		}
		return "s3://" + bucketHint + "/" + key, nil
	}
}

func parseGCSJSONUploadURL(parsed *url.URL) (bucket string, key string, ok bool) {
	if parsed == nil {
		return "", "", false
	}
	q := parsed.Query()
	if strings.TrimSpace(q.Get("uploadType")) != "media" {
		return "", "", false
	}
	key = strings.Trim(strings.TrimSpace(q.Get("name")), "/")
	if key == "" {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(parsed.Path), "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "b" {
			bucket = strings.TrimSpace(parts[i+1])
			break
		}
	}
	if bucket == "" {
		return "", "", false
	}
	return bucket, key, true
}

func parseAzureBlobSignedURL(parsed *url.URL) (bucket string, key string, ok bool) {
	if parsed == nil {
		return "", "", false
	}
	q := parsed.Query()
	if strings.TrimSpace(q.Get("sig")) == "" || !strings.EqualFold(strings.TrimSpace(q.Get("sr")), "b") {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(parsed.Path), "/"), "/")
	if len(parts) < 2 {
		return "", "", false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if strings.Contains(host, ".blob.") {
		bucket = strings.TrimSpace(parts[0])
		key = strings.Join(parts[1:], "/")
	} else {
		// Azurite path shape: /<account>/<container>/<key...>
		if len(parts) < 3 {
			return "", "", false
		}
		bucket = strings.TrimSpace(parts[1])
		key = strings.Join(parts[2:], "/")
	}
	bucket = strings.Trim(bucket, "/")
	key = strings.Trim(key, "/")
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}
