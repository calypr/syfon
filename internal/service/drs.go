package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
)

func ResolveObjectByIDOrChecksum(database db.ObjectStore, ctx context.Context, ident string) (*models.InternalObject, error) {
	if strings.TrimSpace(ident) == "" {
		return nil, common.ErrNotFound
	}

	byChecksum, err := database.GetObjectsByChecksum(ctx, ident)
	if err == nil && len(byChecksum) > 0 {
		objCopy := byChecksum[0]
		return &objCopy, nil
	}

	obj, err := database.GetObject(ctx, ident)
	if err == nil {
		return obj, nil
	}
	if !errors.Is(err, common.ErrNotFound) {
		return nil, err
	}

	canonicalID, aliasErr := database.ResolveObjectAlias(ctx, ident)
	if aliasErr == nil && strings.TrimSpace(canonicalID) != "" {
		obj, getErr := database.GetObject(ctx, canonicalID)
		if getErr == nil {
			objCopy := *obj
			objCopy.DrsObject.Id = ident
			objCopy.DrsObject.SelfUri = "drs://" + ident
			return &objCopy, nil
		}
		if !errors.Is(getErr, common.ErrNotFound) {
			return nil, getErr
		}
	}

	if aliasErr != nil && !errors.Is(aliasErr, common.ErrNotFound) {
		return nil, aliasErr
	}

	return nil, common.ErrNotFound
}

func ResolveBucket(ctx context.Context, database db.CredentialStore, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}

	scopes, err := database.ListBucketScopes(ctx)
	if err == nil {
		for _, scope := range scopes {
			resource := common.ResourcePathForScope(scope.Organization, scope.ProjectID)
			if resource == "" {
				continue
			}
			if authz.HasAnyMethodAccess(ctx, []string{resource}, "file_upload", "create", "update", "read") {
				return scope.Bucket, nil
			}
		}
	}

	creds, err := database.ListS3Credentials(ctx)
	if err != nil || len(creds) == 0 {
		return "", fmt.Errorf("no bucket configured")
	}
	return creds[0].Bucket, nil
}

func FirstSupportedAccessURL(obj *models.InternalObject) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	for _, am := range *obj.AccessMethods {
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
			continue
		}
		u, err := url.Parse(am.AccessUrl.Url)
		if err != nil {
			continue
		}
		if common.ProviderFromScheme(u.Scheme) == "" {
			continue
		}
		return am.AccessUrl.Url
	}
	return ""
}

func S3KeyFromInternalObjectForBucket(obj *models.InternalObject, bucket string) (string, bool) {
	if obj == nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
	if targetBucket == "" {
		return "", false
	}
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil {
				continue
			}
			raw := strings.TrimSpace(am.AccessUrl.Url)
			if raw == "" {
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || !strings.EqualFold(u.Scheme, "s3") {
				continue
			}
			if strings.TrimSpace(u.Host) != targetBucket {
				continue
			}
			key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
			if key != "" {
				return key, true
			}
		}
	}
	return "", false
}

func ResolveObjectRemotePath(database db.ObjectStore, ctx context.Context, objectID string, bucket string) (string, bool) {
	targetBucket := strings.TrimSpace(bucket)
	if objectID == "" || targetBucket == "" {
		return "", false
	}

	obj, err := ResolveObjectByIDOrChecksum(database, ctx, objectID)
	if err != nil {
		return "", false
	}

	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
				continue
			}
			u, err := url.Parse(am.AccessUrl.Url)
			if err != nil {
				continue
			}
			if common.ProviderFromScheme(u.Scheme) == "" {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(u.Host), targetBucket) {
				continue
			}
			key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
			if key != "" {
				return key, true
			}
		}
	}
	return "", false
}
