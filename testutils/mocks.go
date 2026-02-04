package testutils

import (
	"context"
	"errors"
	"fmt"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

// MockDatabase implements core.DatabaseInterface for testing
type MockDatabase struct {
	Objects map[string]*drs.DrsObject
}

func (m *MockDatabase) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	return nil, nil
}

func (m *MockDatabase) GetObject(ctx context.Context, id string) (*drs.DrsObject, error) {
	if obj, ok := m.Objects[id]; ok {
		return obj, nil
	}
	return nil, errors.New("object not found")
}

func (m *MockDatabase) DeleteObject(ctx context.Context, id string) error {
	return nil
}

func (m *MockDatabase) CreateObject(ctx context.Context, obj *drs.DrsObject) error {
	if m.Objects == nil {
		m.Objects = make(map[string]*drs.DrsObject)
	}
	m.Objects[obj.Id] = obj
	return nil
}

func (m *MockDatabase) GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error) {
	return nil, nil
}

func (m *MockDatabase) RegisterObjects(ctx context.Context, objects []drs.DrsObject) error {
	return nil
}

func (m *MockDatabase) GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error) {
	return nil, nil
}

func (m *MockDatabase) BulkDeleteObjects(ctx context.Context, ids []string) error {
	return nil
}

func (m *MockDatabase) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	return nil
}

func (m *MockDatabase) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	return nil
}

func (m *MockDatabase) GetS3Credential(ctx context.Context, bucket string) (*core.S3Credential, error) {
	// Simple mock: return a dummy credential for any bucket
	return &core.S3Credential{
		Bucket:    bucket,
		Region:    "us-east-1",
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}, nil
}

func (m *MockDatabase) SaveS3Credential(ctx context.Context, cred *core.S3Credential) error {
	return nil
}

func (m *MockDatabase) DeleteS3Credential(ctx context.Context, bucket string) error {
	return nil
}

func (m *MockDatabase) ListS3Credentials(ctx context.Context) ([]core.S3Credential, error) {
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
