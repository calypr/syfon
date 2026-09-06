package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
)

type storageTarget struct {
	provider string
	bucket   string
	key      string
	path     string
	location string
}

func (m *ObjectManager) deleteObjectStorage(ctx context.Context, obj *objects.Record) error {
	targets, err := m.storageTargetsForObject(ctx, obj)
	if err != nil {
		return err
	}
	return m.deleteStorageTargets(ctx, targets)
}

func (m *ObjectManager) deleteObjectsStorage(ctx context.Context, objects []objects.Record) error {
	targets := make([]storageTarget, 0, len(objects))
	seen := make(map[string]struct{})
	for i := range objects {
		objectTargets, err := m.storageTargetsForObject(ctx, &objects[i])
		if err != nil {
			return err
		}
		for _, target := range objectTargets {
			key := storageTargetKey(target)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			targets = append(targets, target)
		}
	}
	return m.deleteStorageTargets(ctx, targets)
}

func (m *ObjectManager) deleteStorageTargets(ctx context.Context, targets []storageTarget) error {
	if len(targets) == 0 {
		return nil
	}
	if m.storageDelete == nil {
		return fmt.Errorf("storage deletion is not configured")
	}
	locations := make([]storage.DeleteTarget, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		location := storageTargetLocation(target)
		if strings.TrimSpace(location) == "" {
			continue
		}
		if _, exists := seen[location]; exists {
			continue
		}
		seen[location] = struct{}{}
		locations = append(locations, storage.DeleteTarget{Location: location})
	}
	if err := m.storageDelete.DeleteExact(ctx, locations); err != nil {
		return mapStorageDeleteError(err)
	}
	return nil
}

func (m *ObjectManager) storageTargetsForObject(ctx context.Context, obj *objects.Record) ([]storageTarget, error) {
	if obj == nil || obj.AccessMethods == nil {
		return nil, nil
	}

	targets := make([]storageTarget, 0, len(*obj.AccessMethods))
	seen := make(map[string]struct{}, len(*obj.AccessMethods))
	for _, am := range *obj.AccessMethods {
		if am.AccessUrl == nil || strings.TrimSpace(am.AccessUrl.Url) == "" {
			continue
		}

		rawURL := strings.TrimSpace(am.AccessUrl.Url)
		// Stored locations are physical replicas. Their bucket/key must remain
		// exact through deletion; project scopes are upload-time concerns.
		target, ok, err := m.storageTargetFromURL(ctx, rawURL)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		key := storageTargetKey(target)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

func (m *ObjectManager) storageTargetFromURL(ctx context.Context, raw string) (storageTarget, bool, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return storageTarget{}, false, fmt.Errorf("parse access url %q: %w", raw, err)
	}
	if provider := address.ProviderFromScheme(u.Scheme); provider != "" {
		bucket := strings.TrimSpace(u.Host)
		key := strings.Trim(strings.TrimSpace(u.Path), "/")
		if bucket == "" || key == "" {
			return storageTarget{}, false, nil
		}
		cred, err := m.credentialForBucket(ctx, bucket)
		if err != nil {
			return storageTarget{}, false, fmt.Errorf("lookup credential for bucket %s: %w", bucket, err)
		}
		normalizedProvider := provider
		if cred != nil {
			normalizedProvider = address.NormalizeProvider(cred.Provider, provider)
		}
		if normalizedProvider == address.FileProvider {
			if cred == nil {
				return storageTarget{}, false, fmt.Errorf("file-backed bucket %s requires credential", bucket)
			}
			root := filepath.Clean(strings.TrimSpace(cred.Endpoint))
			if root == "." || root == "" {
				root = strings.TrimPrefix(strings.TrimSpace(cred.Bucket), "/")
			}
			if root == "" {
				return storageTarget{}, false, fmt.Errorf("file-backed bucket %s missing storage root", bucket)
			}
			return storageTarget{
				provider: normalizedProvider,
				bucket:   bucket,
				key:      key,
				path:     filepath.Clean(filepath.Join(root, filepath.FromSlash(key))),
				location: raw,
			}, true, nil
		}
		return storageTarget{provider: normalizedProvider, bucket: bucket, key: key, location: raw}, true, nil
	}

	if filepath.IsAbs(raw) {
		return storageTarget{provider: address.FileProvider, path: filepath.Clean(raw), location: filepath.Clean(raw)}, true, nil
	}
	return storageTarget{}, false, nil
}

func (m *ObjectManager) deleteStorageTarget(ctx context.Context, target storageTarget) error {
	if m.storageDelete == nil {
		return fmt.Errorf("storage deletion is not configured")
	}
	location := storageTargetLocation(target)
	if strings.TrimSpace(location) == "" {
		return nil
	}
	if err := m.storageDelete.DeleteExact(ctx, []storage.DeleteTarget{{Location: location}}); err != nil {
		return mapStorageDeleteError(err)
	}
	return nil
}

func (m *ObjectManager) deleteS3Object(ctx context.Context, bucket, key string) error {
	return m.deleteS3Objects(ctx, bucket, []string{key})
}

func (m *ObjectManager) deleteS3Objects(ctx context.Context, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	targets := make([]storageTarget, 0, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			continue
		}
		targets = append(targets, storageTarget{
			provider: address.S3Provider,
			bucket:   bucket,
			key:      strings.Trim(strings.TrimSpace(key), "/"),
			location: address.BucketToURL(bucket, key),
		})
	}
	return m.deleteStorageTargets(ctx, targets)
}

func storageTargetLocation(target storageTarget) string {
	if strings.TrimSpace(target.location) != "" {
		return strings.TrimSpace(target.location)
	}
	if target.provider == address.FileProvider {
		return strings.TrimSpace(target.path)
	}
	if strings.TrimSpace(target.bucket) == "" || strings.TrimSpace(target.key) == "" {
		return ""
	}
	return address.BucketToURL(target.bucket, target.key)
}

func storageTargetKey(target storageTarget) string {
	return target.provider + "\x00" + target.bucket + "\x00" + target.key + "\x00" + target.path
}

func mapStorageDeleteError(err error) error {
	var operation *storage.OperationError
	if !errors.As(err, &operation) || operation.Cause == nil {
		return err
	}
	return operation.Cause
}
