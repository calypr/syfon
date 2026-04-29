package core

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
)

type contextKey string

var baseURLKey contextKey = "baseURL"

// WithBaseURL adds the base URL to the context.
func WithBaseURL(ctx context.Context, baseURL string) context.Context {
	return context.WithValue(ctx, baseURLKey, baseURL)
}

// GetBaseURL retrieves the base URL from the context.
func GetBaseURL(ctx context.Context) string {
	val, _ := ctx.Value(baseURLKey).(string)
	return val
}

// ObjectManager standardizes object lifecycle operations across all API surfaces.
type ObjectManager struct {
	db db.DatabaseInterface
	uM urlmanager.UrlManager
}

func NewObjectManager(db db.DatabaseInterface, uM urlmanager.UrlManager) *ObjectManager {
	return &ObjectManager{db: db, uM: uM}
}

// GetObject retrieves an internal object by ID, Alias, or Checksum and validates access.
func (m *ObjectManager) GetObject(ctx context.Context, ident string, requiredMethod string) (*models.InternalObject, error) {
	if strings.TrimSpace(ident) == "" {
		return nil, common.ErrNotFound
	}

	if obj, found, err := m.lookupObjectByID(ctx, ident); err != nil {
		return nil, err
	} else if found {
		return m.checkAccessAndReturn(obj, requiredMethod, ctx)
	}

	if obj, found, err := m.lookupObjectByAlias(ctx, ident); err != nil {
		return nil, err
	} else if found {
		return m.checkAccessAndReturn(obj, requiredMethod, ctx)
	}

	if byChecksum, ok := m.lookupObjectByChecksum(ctx, ident); ok {
		return m.checkAccessAndReturn(&byChecksum[0], requiredMethod, ctx)
	}

	return nil, common.ErrNotFound
}

func (m *ObjectManager) lookupObjectByChecksum(ctx context.Context, ident string) ([]models.InternalObject, bool) {
	byChecksum, err := m.db.GetObjectsByChecksum(ctx, ident)
	if err != nil || len(byChecksum) == 0 {
		return nil, false
	}
	return byChecksum, true
}

func (m *ObjectManager) lookupObjectByID(ctx context.Context, ident string) (*models.InternalObject, bool, error) {
	obj, err := m.db.GetObject(ctx, ident)
	if err == nil {
		return obj, true, nil
	}
	if common.IsNotFoundError(err) {
		return nil, false, nil
	}
	return nil, false, err
}

func (m *ObjectManager) lookupObjectByAlias(ctx context.Context, ident string) (*models.InternalObject, bool, error) {
	canonicalID, aliasErr := m.db.ResolveObjectAlias(ctx, ident)
	if aliasErr != nil {
		if common.IsNotFoundError(aliasErr) {
			return nil, false, nil
		}
		return nil, false, aliasErr
	}
	if strings.TrimSpace(canonicalID) == "" {
		return nil, false, nil
	}

	obj, err := m.db.GetObject(ctx, canonicalID)
	if err != nil {
		if common.IsNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	objCopy := *obj
	objCopy.DrsObject.Id = ident
	objCopy.DrsObject.SelfUri = "drs://" + ident
	return &objCopy, true, nil
}

func (m *ObjectManager) checkAccessAndReturn(obj *models.InternalObject, method string, ctx context.Context) (*models.InternalObject, error) {
	if method != "" && !authz.HasMethodAccess(ctx, method, syfoncommon.AuthzMapToList(obj.Authorizations)) {
		return nil, common.ErrUnauthorized
	}
	return obj, nil
}

// RegisterBulk saves multiple internal objects as a single logical operation.
func (m *ObjectManager) RegisterBulk(ctx context.Context, candidates []drs.DrsObjectCandidate) (int, error) {
	now := time.Now().UTC()
	toRegister := make([]models.InternalObject, 0, len(candidates))
	for _, c := range candidates {
		obj, err := CandidateToInternalObject(c, now)
		if err != nil {
			return 0, err
		}
		toRegister = append(toRegister, obj)
	}

	if err := m.db.RegisterObjects(ctx, toRegister); err != nil {
		return 0, err
	}
	return len(toRegister), nil
}

// DeleteBulkByScope removes all objects matching an organization/project scope after verifying permissions.
func (m *ObjectManager) DeleteBulkByScope(ctx context.Context, organization, project string) (int, error) {
	ids, err := m.db.ListObjectIDsByScope(ctx, organization, project)
	if err != nil {
		return 0, err
	}

	toDelete := m.filterDeletableObjectIDs(ctx, ids)

	if len(toDelete) == 0 {
		return 0, nil
	}

	if err := m.db.BulkDeleteObjects(ctx, toDelete); err != nil {
		return 0, err
	}
	return len(toDelete), nil
}

// SignURL generates a signed URL for an object's access method.
func (m *ObjectManager) SignURL(ctx context.Context, accessURL string, options urlmanager.SignOptions) (string, error) {
	return m.uM.SignURL(ctx, m.resolveSigningBucket(ctx, accessURL), accessURL, options)
}

// ResolveBucket validates a bucket name or returns the default one.
func (m *ObjectManager) ResolveBucket(ctx context.Context, bucketName string) (string, error) {
	creds, err := m.ListS3Credentials(ctx)
	if err != nil {
		return "", err
	}
	return resolveBucketName(creds, bucketName)
}

func (m *ObjectManager) SignDownloadPart(ctx context.Context, bucket, accessURL string, start, end int64, options urlmanager.SignOptions) (string, error) {
	return m.uM.SignDownloadPart(ctx, bucket, accessURL, start, end, options)
}

// ResolveObjectRemotePath returns the key for an object in a specific bucket.
func (m *ObjectManager) ResolveObjectRemotePath(ctx context.Context, objectID string, bucket string) (string, bool) {
	obj, err := m.GetObject(ctx, objectID, "")
	if err != nil {
		return "", false
	}
	return S3KeyFromInternalObjectForBucket(obj, bucket)
}

func (m *ObjectManager) DeleteObject(ctx context.Context, id string) error {
	return m.db.DeleteObject(ctx, id)
}

func (m *ObjectManager) BulkDeleteObjects(ctx context.Context, ids []string) error {
	return m.db.BulkDeleteObjects(ctx, ids)
}

func (m *ObjectManager) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	return m.db.UpdateObjectAccessMethods(ctx, objectID, accessMethods)
}

func (m *ObjectManager) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	return m.db.BulkUpdateAccessMethods(ctx, updates)
}

func (m *ObjectManager) RegisterObjects(ctx context.Context, objs []models.InternalObject) error {
	return m.db.RegisterObjects(ctx, objs)
}

func (m *ObjectManager) GetObjectsByChecksums(ctx context.Context, hashes []string) (map[string][]models.InternalObject, error) {
	return m.db.GetObjectsByChecksums(ctx, hashes)
}

// Pass-through operations to DB
func (m *ObjectManager) GetObjectsByChecksum(ctx context.Context, checksum string) ([]models.InternalObject, error) {
	return m.db.GetObjectsByChecksum(ctx, checksum)
}

func (m *ObjectManager) SavePendingLFSMeta(ctx context.Context, entries []models.PendingLFSMeta) error {
	return m.db.SavePendingLFSMeta(ctx, entries)
}

func (m *ObjectManager) GetPendingLFSMeta(ctx context.Context, oid string) (*models.PendingLFSMeta, error) {
	return m.db.GetPendingLFSMeta(ctx, oid)
}

func (m *ObjectManager) PopPendingLFSMeta(ctx context.Context, oid string) (*models.PendingLFSMeta, error) {
	return m.db.PopPendingLFSMeta(ctx, oid)
}

func (m *ObjectManager) ListS3Credentials(ctx context.Context) ([]models.S3Credential, error) {
	return m.db.ListS3Credentials(ctx)
}

func (m *ObjectManager) GetS3Credential(ctx context.Context, bucket string) (*models.S3Credential, error) {
	return m.db.GetS3Credential(ctx, bucket)
}

func (m *ObjectManager) SaveS3Credential(ctx context.Context, cred *models.S3Credential) error {
	return m.db.SaveS3Credential(ctx, cred)
}

func (m *ObjectManager) DeleteS3Credential(ctx context.Context, bucket string) error {
	return m.db.DeleteS3Credential(ctx, bucket)
}

func (m *ObjectManager) ListBucketScopes(ctx context.Context) ([]models.BucketScope, error) {
	return m.db.ListBucketScopes(ctx)
}

func (m *ObjectManager) CreateBucketScope(ctx context.Context, scope *models.BucketScope) error {
	return m.db.CreateBucketScope(ctx, scope)
}

func (m *ObjectManager) InitMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	return m.uM.InitMultipartUpload(ctx, bucket, key)
}

func (m *ObjectManager) SignMultipartPart(ctx context.Context, bucket, key, uploadID string, partNum int32) (string, error) {
	return m.uM.SignMultipartPart(ctx, bucket, key, uploadID, partNum)
}

func (m *ObjectManager) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []urlmanager.MultipartPart) error {
	return m.uM.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (m *ObjectManager) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	return m.db.GetServiceInfo(ctx)
}

func (m *ObjectManager) RecordDownload(ctx context.Context, id string) error {
	return m.db.RecordFileDownload(ctx, id)
}

func (m *ObjectManager) RecordUpload(ctx context.Context, id string) error {
	return m.db.RecordFileUpload(ctx, id)
}

func (m *ObjectManager) RecordTransferAttributionEvents(ctx context.Context, events []models.TransferAttributionEvent) error {
	return m.db.RecordTransferAttributionEvents(ctx, events)
}

func (m *ObjectManager) RecordProviderTransferEvents(ctx context.Context, events []models.ProviderTransferEvent) error {
	return m.db.RecordProviderTransferEvents(ctx, events)
}

func (m *ObjectManager) GetBulkObjects(ctx context.Context, ids []string) ([]models.InternalObject, error) {
	return m.db.GetBulkObjects(ctx, ids)
}

func (m *ObjectManager) CreateObjectAlias(ctx context.Context, aliasID, canonicalID string) error {
	return m.db.CreateObjectAlias(ctx, aliasID, canonicalID)
}

func (m *ObjectManager) ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error) {
	return m.db.ListObjectIDsByScope(ctx, organization, project)
}

func (m *ObjectManager) filterDeletableObjectIDs(ctx context.Context, ids []string) []string {
	toDelete := make([]string, 0, len(ids))
	for _, id := range ids {
		if m.canDeleteObject(ctx, id) {
			toDelete = append(toDelete, id)
		}
	}
	return toDelete
}

func (m *ObjectManager) canDeleteObject(ctx context.Context, id string) bool {
	_, err := m.GetObject(ctx, id, "delete")
	return err == nil
}

func (m *ObjectManager) resolveSigningBucket(ctx context.Context, accessURL string) string {
	if bucket, _, ok := common.ParseS3URL(accessURL); ok {
		return bucket
	}
	if strings.HasPrefix(accessURL, "s3://") {
		parts := strings.Split(strings.TrimPrefix(accessURL, "s3://"), "/")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	if parsed, err := url.Parse(strings.TrimSpace(accessURL)); err == nil && parsed.Scheme == "" && strings.TrimSpace(parsed.Path) != "" {
		if creds, err := m.ListS3Credentials(ctx); err == nil {
			for _, cred := range creds {
				if common.NormalizeProvider(cred.Provider, "") == common.FileProvider {
					return cred.Bucket
				}
			}
		}
	}
	return ""
}

func resolveBucketName(creds []models.S3Credential, bucketName string) (string, error) {
	if len(creds) == 0 {
		return "", fmt.Errorf("no buckets configured")
	}
	if bucketName == "" {
		return creds[0].Bucket, nil
	}
	for _, c := range creds {
		if c.Bucket == bucketName {
			return c.Bucket, nil
		}
	}
	return "", fmt.Errorf("bucket %q not configured", bucketName)
}
