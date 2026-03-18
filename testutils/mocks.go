package testutils

import (
	"context"
	"fmt"
	"strings"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

// MockDatabase implements core.DatabaseInterface for testing
type MockDatabase struct {
	Objects      map[string]*drs.DrsObject
	ObjectAuthz  map[string][]string
	Credentials  map[string]core.S3Credential
	GetObjectErr error
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
		} else {
			wrapped.Authorizations = append([]string(nil), obj.Authorizations...)
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
	if obj, ok := m.Objects[checksum]; ok {
		wrapped := core.InternalObject{DrsObject: *obj}
		if authz, ok := m.ObjectAuthz[checksum]; ok {
			wrapped.Authorizations = append([]string(nil), authz...)
		}
		return []core.InternalObject{wrapped}, nil
	}
	return []core.InternalObject{}, nil
}

func (m *MockDatabase) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]core.InternalObject, error) {
	out := make(map[string][]core.InternalObject, len(checksums))
	for _, cs := range checksums {
		if m.Objects == nil {
			out[cs] = nil
			continue
		}
		if obj, ok := m.Objects[cs]; ok {
			wrapped := core.InternalObject{DrsObject: *obj}
			if authz, ok := m.ObjectAuthz[cs]; ok {
				wrapped.Authorizations = append([]string(nil), authz...)
			}
			out[cs] = []core.InternalObject{wrapped}
			continue
		}
		out[cs] = nil
	}
	return out, nil
}

func (m *MockDatabase) ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error) {
	ids := make([]string, 0)
	for id, obj := range m.Objects {
		authz := obj.Authorizations
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
	return []core.S3Credential{
		{Bucket: "test-bucket-1", Region: "us-east-1"},
	}, nil
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

func (m *MockUrlManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []urlmanager.MultipartPart) error {
	return nil
}
