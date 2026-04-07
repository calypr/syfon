package testutils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
)

// MockDatabase implements core.DatabaseInterface for testing
type MockDatabase struct {
	Objects        map[string]*drs.DrsObject
	ObjectAuthz    map[string][]string
	Credentials    map[string]core.S3Credential
	BucketScopes   map[string]core.BucketScope
	PendingMeta    map[string]core.PendingLFSMeta
	Usage          map[string]core.FileUsage
	NoDefaultCreds bool
	GetObjectErr   error
}

func (m *MockDatabase) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	return nil, nil
}

func (m *MockDatabase) GetObject(ctx context.Context, id string) (*core.InternalObject, error) {
	if m.GetObjectErr != nil {
		return nil, m.GetObjectErr
	}
	if obj, ok := m.Objects[id]; ok {
		wrapped := core.InternalObject{DrsObject: *obj}
		if authz, ok := m.ObjectAuthz[id]; ok {
			wrapped.Authorizations = append([]string(nil), authz...)
		}
		return &wrapped, nil
	}
	return nil, fmt.Errorf("%w: object not found", core.ErrNotFound)
}

func (m *MockDatabase) DeleteObject(ctx context.Context, id string) error {
	if m.Objects != nil {
		delete(m.Objects, id)
	}
	return nil
}

func (m *MockDatabase) CreateObject(ctx context.Context, obj *core.InternalObject) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*drs.DrsObject)
	}
	copyObj := obj.DrsObject
	m.Objects[obj.Id] = &copyObj
	if len(obj.Authorizations) > 0 {
		if m.ObjectAuthz == nil {
			m.ObjectAuthz = make(map[string][]string)
		}
		m.ObjectAuthz[obj.Id] = append([]string(nil), obj.Authorizations...)
	}
	return nil
}

func (m *MockDatabase) GetObjectsByChecksum(ctx context.Context, checksum string) ([]core.InternalObject, error) {
	if m.Objects == nil {
		return []core.InternalObject{}, nil
	}
	out := make([]core.InternalObject, 0, 1)
	for id, obj := range m.Objects {
		if id == checksum || obj.Id == checksum {
			wrapped := core.InternalObject{DrsObject: *obj}
			if authz, ok := m.ObjectAuthz[id]; ok {
				wrapped.Authorizations = append([]string(nil), authz...)
			}
			out = append(out, wrapped)
			continue
		}
		for _, c := range obj.Checksums {
			if strings.EqualFold(strings.TrimSpace(c.Checksum), strings.TrimSpace(checksum)) {
				wrapped := core.InternalObject{DrsObject: *obj}
				if authz, ok := m.ObjectAuthz[id]; ok {
					wrapped.Authorizations = append([]string(nil), authz...)
				}
				out = append(out, wrapped)
				break
			}
		}
	}
	return out, nil
}

func (m *MockDatabase) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]core.InternalObject, error) {
	out := make(map[string][]core.InternalObject, len(checksums))
	for _, cs := range checksums {
		matches, err := m.GetObjectsByChecksum(ctx, cs)
		if err != nil {
			return nil, err
		}
		out[cs] = matches
	}
	return out, nil
}

func (m *MockDatabase) ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error) {
	ids := make([]string, 0)
	for id := range m.Objects {
		authz := []string{}
		if m.ObjectAuthz != nil {
			if v, ok := m.ObjectAuthz[id]; ok {
				authz = v
			}
		}
		for _, r := range authz {
			if r == resourcePrefix || strings.HasPrefix(r, resourcePrefix+"/") {
				ids = append(ids, id)
				break
			}
		}
	}
	return ids, nil
}

func (m *MockDatabase) CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error {
	if m.Objects == nil {
		return fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	obj, ok := m.Objects[canonicalObjectID]
	if !ok {
		return fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	copyObj := *obj
	copyObj.Id = aliasID
	m.Objects[aliasID] = &copyObj
	if m.ObjectAuthz != nil {
		if authz, ok := m.ObjectAuthz[canonicalObjectID]; ok {
			m.ObjectAuthz[aliasID] = append([]string(nil), authz...)
		}
	}
	return nil
}

func (m *MockDatabase) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	if m.Objects != nil {
		if _, ok := m.Objects[aliasID]; ok {
			return aliasID, nil
		}
	}
	return "", fmt.Errorf("%w: object not found", core.ErrNotFound)
}

func (m *MockDatabase) RegisterObjects(ctx context.Context, objects []core.InternalObject) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*drs.DrsObject)
	}
	for _, obj := range objects {
		copyObj := obj.DrsObject
		m.Objects[obj.Id] = &copyObj
		if m.ObjectAuthz == nil {
			m.ObjectAuthz = make(map[string][]string)
		}
		m.ObjectAuthz[obj.Id] = append([]string(nil), obj.Authorizations...)
	}
	return nil
}

func (m *MockDatabase) GetBulkObjects(ctx context.Context, ids []string) ([]core.InternalObject, error) {
	out := make([]core.InternalObject, 0, len(ids))
	for _, id := range ids {
		if obj, ok := m.Objects[id]; ok {
			wrapped := core.InternalObject{DrsObject: *obj}
			if authz, ok := m.ObjectAuthz[id]; ok {
				wrapped.Authorizations = append([]string(nil), authz...)
			}
			out = append(out, wrapped)
		}
	}
	return out, nil
}

func (m *MockDatabase) BulkDeleteObjects(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if m.Objects != nil {
			delete(m.Objects, id)
		}
	}
	return nil
}

func (m *MockDatabase) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*drs.DrsObject)
	}
	obj, ok := m.Objects[objectID]
	if !ok {
		obj = &drs.DrsObject{Id: objectID}
		m.Objects[objectID] = obj
	}
	obj.AccessMethods = accessMethods
	return nil
}

func (m *MockDatabase) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	return nil
}

func (m *MockDatabase) GetS3Credential(ctx context.Context, bucket string) (*core.S3Credential, error) {
	if m.Credentials != nil {
		if cred, ok := m.Credentials[bucket]; ok {
			c := cred
			return &c, nil
		}
	}
	return &core.S3Credential{
		Bucket:    bucket,
		Provider:  "s3",
		Region:    "us-east-1",
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}, nil
}

func (m *MockDatabase) SaveS3Credential(ctx context.Context, cred *core.S3Credential) error {
	if m.Credentials == nil {
		m.Credentials = make(map[string]core.S3Credential)
	}
	m.Credentials[cred.Bucket] = *cred
	return nil
}

func (m *MockDatabase) DeleteS3Credential(ctx context.Context, bucket string) error {
	if m.Credentials != nil {
		delete(m.Credentials, bucket)
	}
	return nil
}

func (m *MockDatabase) ListS3Credentials(ctx context.Context) ([]core.S3Credential, error) {
	if len(m.Credentials) > 0 {
		out := make([]core.S3Credential, 0, len(m.Credentials))
		for _, v := range m.Credentials {
			out = append(out, v)
		}
		return out, nil
	}
	if m.NoDefaultCreds {
		return []core.S3Credential{}, nil
	}
	return []core.S3Credential{
		{Bucket: "test-bucket-1", Provider: "s3", Region: "us-east-1"},
	}, nil
}

func bucketScopeKey(org, project string) string {
	return strings.TrimSpace(org) + "|" + strings.TrimSpace(project)
}

func (m *MockDatabase) CreateBucketScope(ctx context.Context, scope *core.BucketScope) error {
	if scope == nil {
		return fmt.Errorf("scope is required")
	}
	if m.BucketScopes == nil {
		m.BucketScopes = make(map[string]core.BucketScope)
	}
	k := bucketScopeKey(scope.Organization, scope.ProjectID)
	if existing, ok := m.BucketScopes[k]; ok {
		if existing.Bucket == scope.Bucket && strings.Trim(existing.PathPrefix, "/") == strings.Trim(scope.PathPrefix, "/") {
			return nil
		}
		return fmt.Errorf("%w: scope already exists", core.ErrConflict)
	}
	m.BucketScopes[k] = *scope
	return nil
}

func (m *MockDatabase) GetBucketScope(ctx context.Context, organization, projectID string) (*core.BucketScope, error) {
	if m.BucketScopes == nil {
		return nil, fmt.Errorf("%w: bucket scope not found", core.ErrNotFound)
	}
	k := bucketScopeKey(organization, projectID)
	s, ok := m.BucketScopes[k]
	if !ok {
		return nil, fmt.Errorf("%w: bucket scope not found", core.ErrNotFound)
	}
	cp := s
	return &cp, nil
}

func (m *MockDatabase) ListBucketScopes(ctx context.Context) ([]core.BucketScope, error) {
	out := make([]core.BucketScope, 0, len(m.BucketScopes))
	for _, s := range m.BucketScopes {
		out = append(out, s)
	}
	return out, nil
}

func (m *MockDatabase) SavePendingLFSMeta(ctx context.Context, entries []core.PendingLFSMeta) error {
	if m.PendingMeta == nil {
		m.PendingMeta = make(map[string]core.PendingLFSMeta)
	}
	for _, e := range entries {
		m.PendingMeta[e.OID] = e
	}
	return nil
}

func (m *MockDatabase) GetPendingLFSMeta(ctx context.Context, oid string) (*core.PendingLFSMeta, error) {
	if m.PendingMeta == nil {
		return nil, fmt.Errorf("%w: pending metadata not found", core.ErrNotFound)
	}
	e, ok := m.PendingMeta[oid]
	if !ok {
		return nil, fmt.Errorf("%w: pending metadata not found", core.ErrNotFound)
	}
	return &e, nil
}

func (m *MockDatabase) PopPendingLFSMeta(ctx context.Context, oid string) (*core.PendingLFSMeta, error) {
	if m.PendingMeta == nil {
		return nil, fmt.Errorf("%w: pending metadata not found", core.ErrNotFound)
	}
	e, ok := m.PendingMeta[oid]
	if !ok {
		return nil, fmt.Errorf("%w: pending metadata not found", core.ErrNotFound)
	}
	delete(m.PendingMeta, oid)
	return &e, nil
}

func (m *MockDatabase) RecordFileUpload(ctx context.Context, objectID string) error {
	if m.Usage == nil {
		m.Usage = make(map[string]core.FileUsage)
	}
	u := m.Usage[objectID]
	u.ObjectID = objectID
	u.UploadCount++
	now := time.Now().UTC()
	u.LastUploadTime = &now
	if obj, ok := m.Objects[objectID]; ok {
		u.Name = obj.Name
		u.Size = obj.Size
	}
	if u.LastAccessTime == nil || now.After(*u.LastAccessTime) {
		t := now
		u.LastAccessTime = &t
	}
	m.Usage[objectID] = u
	return nil
}

func (m *MockDatabase) RecordFileDownload(ctx context.Context, objectID string) error {
	if m.Usage == nil {
		m.Usage = make(map[string]core.FileUsage)
	}
	u := m.Usage[objectID]
	u.ObjectID = objectID
	u.DownloadCount++
	now := time.Now().UTC()
	u.LastDownloadTime = &now
	if obj, ok := m.Objects[objectID]; ok {
		u.Name = obj.Name
		u.Size = obj.Size
	}
	if u.LastAccessTime == nil || now.After(*u.LastAccessTime) {
		t := now
		u.LastAccessTime = &t
	}
	m.Usage[objectID] = u
	return nil
}

func (m *MockDatabase) GetFileUsage(ctx context.Context, objectID string) (*core.FileUsage, error) {
	if m.Usage == nil {
		return nil, fmt.Errorf("%w: file usage not found", core.ErrNotFound)
	}
	u, ok := m.Usage[objectID]
	if !ok {
		return nil, fmt.Errorf("%w: file usage not found", core.ErrNotFound)
	}
	copyUsage := u
	return &copyUsage, nil
}

func (m *MockDatabase) ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]core.FileUsage, error) {
	out := make([]core.FileUsage, 0)
	if m.Usage == nil {
		return out, nil
	}
	for _, u := range m.Usage {
		if inactiveSince != nil {
			if u.LastDownloadTime != nil && !u.LastDownloadTime.Before(*inactiveSince) {
				continue
			}
		}
		out = append(out, u)
	}
	if offset >= len(out) {
		return []core.FileUsage{}, nil
	}
	if limit <= 0 {
		return out[offset:], nil
	}
	end := offset + limit
	if end > len(out) {
		end = len(out)
	}
	return out[offset:end], nil
}

func (m *MockDatabase) GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (core.FileUsageSummary, error) {
	var s core.FileUsageSummary
	s.TotalFiles = int64(len(m.Objects))
	for _, u := range m.Usage {
		s.TotalUploads += u.UploadCount
		s.TotalDownloads += u.DownloadCount
		if inactiveSince == nil {
			continue
		}
		if u.LastDownloadTime == nil || u.LastDownloadTime.Before(*inactiveSince) {
			s.InactiveFileCount++
		}
	}
	return s, nil
}

// MockUrlManager implements urlmanager.UrlManager for testing
type MockUrlManager struct{}

func (m *MockUrlManager) SignURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	return url + "?signed=true", nil
}

func (m *MockUrlManager) SignUploadURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	return url + "?signed=true&upload=true", nil
}

func (m *MockUrlManager) InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error) {
	return "mock-upload-id", nil
}

func (m *MockUrlManager) SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error) {
	return fmt.Sprintf("s3://%s/%s?uploadId=%s&partNumber=%d", bucket, key, uploadId, partNumber), nil
}

func (m *MockUrlManager) SignDownloadPart(ctx context.Context, accessId string, url string, start int64, end int64, opts urlmanager.SignOptions) (string, error) {
	return fmt.Sprintf("%s?signed=true&range=%d-%d", url, start, end), nil
}

func (m *MockUrlManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []urlmanager.MultipartPart) error {
	return nil
}
