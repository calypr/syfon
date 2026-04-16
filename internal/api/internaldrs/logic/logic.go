package logic

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/provider"
)

const BucketControlResource = "/services/internal/buckets"

var sha256Like = regexp.MustCompile(`^[A-Fa-f0-9]{64}$`)

func HasAnyMethodAccess(ctx *http.Request, resources []string, methods ...string) bool {
	if !core.IsGen3Mode(ctx.Context()) {
		return true
	}
	if len(resources) == 0 {
		return true
	}
	for _, m := range methods {
		if core.HasMethodAccess(ctx.Context(), m, resources) {
			return true
		}
	}
	return false
}

func HasGlobalBucketControlAccess(r *http.Request, methods ...string) bool {
	return HasAnyMethodAccess(r, []string{BucketControlResource}, methods...)
}

func ScopeResource(org, project string) string {
	return strings.TrimSpace(core.ResourcePathForScope(org, project))
}

func HasScopedBucketAccess(r *http.Request, scope core.BucketScope, methods ...string) bool {
	res := ScopeResource(scope.Organization, scope.ProjectID)
	if res == "" {
		return false
	}
	return HasAnyMethodAccess(r, []string{res}, methods...)
}

func ParseScopeQueryFromParams(params internalapi.InternalListParams) (string, bool, error) {
	authz := ""
	if params.Authz != nil {
		authz = strings.TrimSpace(*params.Authz)
	}
	if authz != "" {
		return authz, true, nil
	}
	org := ""
	if params.Organization != nil {
		org = strings.TrimSpace(*params.Organization)
	}
	if org == "" && params.Program != nil {
		org = strings.TrimSpace(*params.Program)
	}
	project := ""
	if params.Project != nil {
		project = strings.TrimSpace(*params.Project)
	}
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func ParseScopeQueryFromDeleteParams(params internalapi.InternalDeleteByQueryParams) (string, bool, error) {
	authz := ""
	if params.Authz != nil {
		authz = strings.TrimSpace(*params.Authz)
	}
	if authz != "" {
		return authz, true, nil
	}
	org := ""
	if params.Organization != nil {
		org = strings.TrimSpace(*params.Organization)
	}
	if org == "" && params.Program != nil {
		org = strings.TrimSpace(*params.Program)
	}
	project := ""
	if params.Project != nil {
		project = strings.TrimSpace(*params.Project)
	}
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func PaginateRecords(records []internalapi.InternalRecord, offset, limit int) []internalapi.InternalRecord {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = 0
	}
	if offset >= len(records) {
		return []internalapi.InternalRecord{}
	}
	end := offset + limit
	if end > len(records) {
		end = len(records)
	}
	return append([]internalapi.InternalRecord(nil), records[offset:end]...)
}

func NormalizeScopePath(rawPath, bucket string) (string, error) {
	p := strings.TrimSpace(rawPath)
	if p == "" {
		return "", nil
	}
	if !strings.HasPrefix(strings.ToLower(p), config.S3Prefix) {
		return "", fmt.Errorf("path must use %s<bucket>/<prefix> format", config.S3Prefix)
	}
	u, err := url.Parse(p)
	if err != nil {
		return "", fmt.Errorf("invalid s3 path: %w", err)
	}
	if !strings.EqualFold(u.Scheme, "s3") || strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("invalid s3 path")
	}
	if !strings.EqualFold(strings.TrimSpace(u.Host), strings.TrimSpace(bucket)) {
		return "", fmt.Errorf("s3 path bucket does not match bucket")
	}
	return strings.Trim(strings.TrimSpace(u.Path), "/"), nil
}

func ProviderForCredential(cred *core.S3Credential) string {
	if cred == nil {
		return provider.S3
	}
	return provider.Normalize(cred.Provider, provider.S3)
}

func ObjectURLForCredential(cred *core.S3Credential, key string) (string, error) {
	if cred == nil {
		return "", fmt.Errorf("credential is required")
	}
	cleanKey := strings.TrimPrefix(strings.TrimSpace(key), "/")
	switch ProviderForCredential(cred) {
	case provider.S3:
		return fmt.Sprintf("%s%s/%s", config.S3Prefix, cred.Bucket, cleanKey), nil
	case provider.GCS:
		return fmt.Sprintf("%s%s/%s", config.GCSPrefix, cred.Bucket, cleanKey), nil
	case provider.Azure:
		return fmt.Sprintf("%s%s/%s", config.AzurePrefix, cred.Bucket, cleanKey), nil
	case provider.File:
		root := strings.TrimSpace(cred.Endpoint)
		if root != "" {
			root = strings.TrimSuffix(root, "/")
			return fmt.Sprintf("%s/%s", root, cleanKey), nil
		}
		return fmt.Sprintf("%s%s/%s", config.FilePrefix, strings.TrimPrefix(cred.Bucket, "/"), cleanKey), nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", ProviderForCredential(cred))
	}
}

func ParseHashQuery(rawHash string, rawType string) (string, string) {
	hashType := normalizeHashQueryType(rawType)
	hashValue := normalizeHashQueryValue(rawHash)
	cleanHash := strings.Trim(strings.TrimSpace(rawHash), `"'`)
	if hashType == "" {
		if parts := strings.SplitN(cleanHash, ":", 2); len(parts) == 2 {
			hashType = normalizeHashQueryType(parts[0])
		}
	}
	return hashType, hashValue
}

func ObjectHasChecksumTypeAndValue(obj core.InternalObject, hashType string, hashValue string) bool {
	if hashType == "" {
		return true
	}
	targetType := normalizeHashQueryType(hashType)
	targetValue := normalizeHashQueryValue(hashValue)
	if targetType == "" || targetValue == "" {
		return false
	}
	for _, checksum := range obj.Checksums {
		if normalizeHashQueryType(checksum.Type) == targetType && normalizeHashQueryValue(checksum.Checksum) == targetValue {
			return true
		}
	}
	return false
}

func LooksLikeSHA256(v string) bool {
	return sha256Like.MatchString(strings.TrimSpace(v))
}

func ChecksumHintFromInputs(guid, fileName string) string {
	if LooksLikeSHA256(guid) {
		return strings.TrimSpace(guid)
	}
	if LooksLikeSHA256(fileName) {
		return strings.TrimSpace(fileName)
	}
	return ""
}

func TargetResourcesFromObject(obj *core.InternalObject) []string {
	if obj == nil || len(obj.Authorizations) == 0 {
		return []string{"/data_file"}
	}
	return append([]string(nil), obj.Authorizations...)
}

func FirstAuthz(authz []string) string {
	if len(authz) == 0 {
		return ""
	}
	return strings.TrimSpace(authz[0])
}

func normalizeHashQueryValue(raw string) string {
	clean := strings.Trim(strings.TrimSpace(raw), `"'`)
	if parts := strings.SplitN(clean, ":", 2); len(parts) == 2 {
		return strings.Trim(strings.TrimSpace(parts[1]), `"'`)
	}
	return clean
}

func normalizeHashQueryType(raw string) string {
	clean := strings.Trim(strings.TrimSpace(raw), `"'`)
	clean = strings.ToLower(clean)
	clean = strings.ReplaceAll(clean, "-", "")
	return clean
}

func ResolveBucket(ctx *http.Request, database core.CredentialStore, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}

	scopes, err := database.ListBucketScopes(ctx.Context())
	if err == nil {
		for _, scope := range scopes {
			resource := ScopeResource(scope.Organization, scope.ProjectID)
			if resource == "" {
				continue
			}
			if HasAnyMethodAccess(ctx, []string{resource}, "file_upload", "create", "update", "read") {
				return scope.Bucket, nil
			}
		}
	}

	creds, err := database.ListS3Credentials(ctx.Context())
	if err != nil || len(creds) == 0 {
		return "", fmt.Errorf("no bucket configured")
	}
	return creds[0].Bucket, nil
}

func ResolveObjectRemotePath(database core.ObjectStore, ctx *http.Request, objectID string, bucket string) (string, bool) {
	if strings.TrimSpace(objectID) == "" || strings.TrimSpace(bucket) == "" {
		return "", false
	}
	obj, err := ResolveObjectByIDOrChecksum(database, ctx.Context(), objectID)
	if err != nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
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
			if err != nil {
				continue
			}
			if provider.FromScheme(u.Scheme) == "" {
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

func ResolveObjectRemotePathWithCtx(database core.ObjectStore, ctx context.Context, objectID string, bucket string) (string, bool) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	return ResolveObjectRemotePath(database, req, objectID, bucket)
}

func ResolveObjectByIDOrChecksum(database core.ObjectStore, ctx context.Context, objectID string) (*core.InternalObject, error) {
	byChecksum, err := database.GetObjectsByChecksum(ctx, objectID)
	if err != nil {
		return nil, err
	}
	if len(byChecksum) > 0 {
		objCopy := byChecksum[0]
		return &objCopy, nil
	}

	obj, err := database.GetObject(ctx, objectID)
	if err == nil {
		return obj, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return nil, err
	}
	canonicalID, aliasErr := database.ResolveObjectAlias(ctx, objectID)
	if aliasErr == nil && strings.TrimSpace(canonicalID) != "" {
		obj, getErr := database.GetObject(ctx, canonicalID)
		if getErr == nil {
			objCopy := *obj
			objCopy.DrsObject.Id = objectID
			objCopy.DrsObject.SelfUri = "drs://" + objectID
			return &objCopy, nil
		}
		if !errors.Is(getErr, core.ErrNotFound) {
			return nil, getErr
		}
	}
	if aliasErr != nil && !errors.Is(aliasErr, core.ErrNotFound) {
		return nil, aliasErr
	}
	return nil, core.ErrNotFound
}

func NilToRequest(ctx context.Context) *http.Request {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	return req
}

func AuthStatusCodeForContext(ctx context.Context) int {
	if core.IsGen3Mode(ctx) && !core.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func AuthStatusCodeForRequest(r *http.Request) int {
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func SortInternalObjects(objs []core.InternalObject) {
	sort.Slice(objs, func(i, j int) bool { return objs[i].Id < objs[j].Id })
}
