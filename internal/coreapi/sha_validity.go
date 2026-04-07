package coreapi

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/calypr/syfon/db/core"
)

var ErrNoValidSHA256 = errors.New("no valid sha256 values provided")

func ComputeSHA256Validity(ctx context.Context, database core.DatabaseInterface, values []string) (map[string]bool, error) {
	targets := NormalizeSHA256(values)
	if len(targets) == 0 {
		return nil, ErrNoValidSHA256
	}

	bucketSet, err := getRegisteredBucketSet(ctx, database)
	if err != nil {
		return nil, err
	}

	objsMap, err := database.GetObjectsByChecksums(ctx, targets)
	if err != nil {
		return nil, err
	}

	resp := make(map[string]bool, len(targets))
	for _, sha := range targets {
		resp[sha] = false
		for _, obj := range objsMap[sha] {
			if hasValidRegisteredS3Target(obj, bucketSet) {
				resp[sha] = true
				break
			}
		}
	}

	return resp, nil
}

func NormalizeSHA256(values []string) []string {
	targets := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		sha := strings.ToLower(strings.TrimSpace(normalizeOID(strings.TrimSpace(raw))))
		if sha == "" {
			continue
		}
		if _, ok := seen[sha]; ok {
			continue
		}
		seen[sha] = struct{}{}
		targets = append(targets, sha)
	}
	return targets
}

func normalizeOID(oid string) string {
	if strings.HasPrefix(oid, "sha256:") {
		return strings.TrimPrefix(oid, "sha256:")
	}
	return oid
}

func getRegisteredBucketSet(ctx context.Context, database core.DatabaseInterface) (map[string]struct{}, error) {
	creds, err := database.ListS3Credentials(ctx)
	if err != nil {
		return nil, err
	}
	registered := make(map[string]struct{}, len(creds))
	for _, c := range creds {
		bucket := strings.TrimSpace(c.Bucket)
		if bucket == "" {
			continue
		}
		registered[bucket] = struct{}{}
	}
	return registered, nil
}

func hasValidRegisteredS3Target(obj core.InternalObject, registeredBuckets map[string]struct{}) bool {
	for _, method := range obj.AccessMethods {
		if !strings.EqualFold(method.Type, "s3") {
			continue
		}
		bucket, key, ok := parseS3URL(method.AccessUrl.Url)
		if !ok || key == "" {
			continue
		}
		if _, found := registeredBuckets[bucket]; !found {
			continue
		}
		return true
	}
	return false
}

func parseS3URL(raw string) (bucket string, key string, ok bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", false
	}
	if !strings.EqualFold(u.Scheme, "s3") {
		return "", "", false
	}
	bucket = strings.TrimSpace(u.Host)
	key = strings.TrimSpace(strings.TrimPrefix(u.Path, "/"))
	if bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}
