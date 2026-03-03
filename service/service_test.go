package service

import (
	"context"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/testutils"
)

func TestGetAccessURL(t *testing.T) {
	// Setup mock DB
	mockDB := &testutils.MockDatabase{
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
	mockUrlManager := &testutils.MockUrlManager{}

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

func TestPostAccessURL(t *testing.T) {
	mockDB := &testutils.MockDatabase{
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
	mockUrlManager := &testutils.MockUrlManager{}
	service := NewObjectsAPIService(mockDB, mockUrlManager)

	ctx := context.Background()
	resp, err := service.PostAccessURL(ctx, "test-obj-id", "test-access-id", drs.PostAccessUrlRequest{})
	if err != nil {
		t.Fatalf("PostAccessURL failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}

func TestRegisterObjects(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	mockUM := &testutils.MockUrlManager{}
	service := NewObjectsAPIService(mockDB, mockUM)

	req := drs.RegisterObjectsRequest{
		Candidates: []drs.DrsObjectCandidate{
			{
				Name: "new-obj",
				Size: 100,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "abc123"},
				},
			},
		},
	}
	resp, err := service.RegisterObjects(context.Background(), req)
	if err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}
func TestGetObject(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-obj": {Id: "test-obj", Size: 500},
		},
	}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	resp, err := service.GetObject(context.Background(), "test-obj", false)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}

func TestBulkUpdateAccessMethods(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	service := NewObjectsAPIService(mockDB, &testutils.MockUrlManager{})

	req := drs.BulkAccessMethodUpdateRequest{
		Updates: []drs.BulkAccessMethodUpdateRequestUpdatesInner{
			{
				ObjectId: "obj-1",
				AccessMethods: []drs.AccessMethod{
					{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://b/k"}},
				},
			},
		},
	}
	resp, err := service.BulkUpdateAccessMethods(context.Background(), req)
	if err != nil {
		t.Fatalf("BulkUpdateAccessMethods failed: %v", err)
	}
	if resp.Code != 200 {
		t.Errorf("expected 200, got %d", resp.Code)
	}
}
