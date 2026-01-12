package service

import (
	"context"
	"errors"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
)

// MockDatabase implements db.DatabaseInterface for testing
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
	return nil
}

func (m *MockDatabase) GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error) {
	return nil, nil
}

// MockUrlManager implements urlmanager.UrlManager for testing
type MockUrlManager struct{}

func (m *MockUrlManager) SignURL(ctx context.Context, resourceName string, url string) (string, error) {
	return url + "?signed=true", nil
}

func TestGetAccessURL(t *testing.T) {
	// Setup mock DB
	mockDB := &MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj-id": {
				Id: "test-obj-id",
				AccessMethods: []drs.AccessMethod{
					{
						AccessId: "test-access-id",
						Type:     "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/key",
						},
					},
				},
			},
		},
	}

	// Setup mock UrlManager
	mockUrlManager := &MockUrlManager{}

	// Create service
	service := NewObjectsAPIService(mockDB, mockUrlManager)

	// Test GetAccessURL
	ctx := context.Background()
	resp, err := service.GetAccessURL(ctx, "test-obj-id", "test-access-id")
	if err != nil {
		t.Fatalf("GetAccessURL failed: %v", err)
	}

	if resp.Code != 200 {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	accessURL, ok := resp.Body.(drs.AccessMethodAccessUrl)
	if !ok {
		t.Fatalf("expected body to be AccessMethodAccessUrl, got %T", resp.Body)
	}

	expectedURL := "s3://bucket/key?signed=true"
	if accessURL.Url != expectedURL {
		t.Errorf("expected URL %s, got %s", expectedURL, accessURL.Url)
	}
}
