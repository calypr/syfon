package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/urlmanager"
)

type coreTestDB struct {
	*testutils.MockDatabase
	aliases map[string]string
	creds   []models.S3Credential
}

func (d *coreTestDB) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	if d.aliases != nil {
		if canonical, ok := d.aliases[aliasID]; ok {
			return canonical, nil
		}
	}
	return "", fmt.Errorf("%w: object not found", common.ErrNotFound)
}

func (d *coreTestDB) ListS3Credentials(ctx context.Context) ([]models.S3Credential, error) {
	if d.creds != nil {
		out := make([]models.S3Credential, len(d.creds))
		copy(out, d.creds)
		return out, nil
	}
	if d.MockDatabase == nil {
		return nil, nil
	}
	return d.MockDatabase.ListS3Credentials(ctx)
}

type capturingURLManager struct {
	signURLBucket       string
	signURLAccessURL    string
	signUploadBucket    string
	signUploadAccessURL string
	signDownloadBucket  string
	signDownloadURL     string
	initBucket          string
	initKey             string
	partBucket          string
	partKey             string
	partUploadID        string
	partNumber          int32
	completeBucket      string
	completeKey         string
	completeUploadID    string
	completeParts       []urlmanager.MultipartPart
}

func (m *capturingURLManager) SignURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	m.signURLBucket = accessId
	m.signURLAccessURL = url
	return "signed:" + url, nil
}

func (m *capturingURLManager) SignUploadURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	m.signUploadBucket = accessId
	m.signUploadAccessURL = url
	return "upload:" + url, nil
}

func (m *capturingURLManager) SignDownloadPart(ctx context.Context, bucket string, url string, start int64, end int64, opts urlmanager.SignOptions) (string, error) {
	m.signDownloadBucket = bucket
	m.signDownloadURL = url
	return "download:" + url, nil
}

func (m *capturingURLManager) InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error) {
	m.initBucket = bucket
	m.initKey = key
	return "upload-id", nil
}

func (m *capturingURLManager) SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error) {
	m.partBucket = bucket
	m.partKey = key
	m.partUploadID = uploadId
	m.partNumber = partNumber
	return "part:" + key, nil
}

func (m *capturingURLManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []urlmanager.MultipartPart) error {
	m.completeBucket = bucket
	m.completeKey = key
	m.completeUploadID = uploadId
	m.completeParts = append([]urlmanager.MultipartPart(nil), parts...)
	return nil
}

func buildGen3Context(privileges map[string]map[string]bool) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, common.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, common.UserPrivilegesKey, privileges)
	return ctx
}

func TestObjectManagerGetObjectLookupPaths(t *testing.T) {
	cases := []struct {
		name    string
		db      *coreTestDB
		ident   string
		method  string
		ctx     context.Context
		wantID  string
		wantURI string
		wantErr error
	}{
		{
			name: "checksum lookup",
			db: &coreTestDB{
				MockDatabase: &testutils.MockDatabase{
					Objects: map[string]*drs.DrsObject{
						"obj-1": {
							Id:      "obj-1",
							SelfUri: "drs://obj-1",
							Checksums: []drs.Checksum{
								{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
							},
						},
					},
					ObjectAuthz: map[string]map[string][]string{
						"obj-1": {"data_file": {}},
					},
				},
			},
			ident:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			method:  "read",
			ctx:     buildGen3Context(map[string]map[string]bool{"/programs/data_file": {"read": true}}),
			wantID:  "obj-1",
			wantURI: "drs://obj-1",
		},
		{
			name: "direct id lookup",
			db: &coreTestDB{
				MockDatabase: &testutils.MockDatabase{
					Objects: map[string]*drs.DrsObject{
						"obj-2": {Id: "obj-2", SelfUri: "drs://obj-2"},
					},
					ObjectAuthz: map[string]map[string][]string{
						"obj-2": {"data_file": {}},
					},
				},
			},
			ident:   "obj-2",
			method:  "read",
			ctx:     buildGen3Context(map[string]map[string]bool{"/programs/data_file": {"read": true}}),
			wantID:  "obj-2",
			wantURI: "drs://obj-2",
		},
		{
			name: "alias fallback",
			db: &coreTestDB{
				MockDatabase: &testutils.MockDatabase{
					Objects: map[string]*drs.DrsObject{
						"canonical-1": {Id: "canonical-1", SelfUri: "drs://canonical-1"},
					},
					ObjectAuthz: map[string]map[string][]string{
						"canonical-1": {"data_file": {}},
					},
				},
				aliases: map[string]string{
					"alias-1": "canonical-1",
				},
			},
			ident:   "alias-1",
			method:  "read",
			ctx:     buildGen3Context(map[string]map[string]bool{"/programs/data_file": {"read": true}}),
			wantID:  "alias-1",
			wantURI: "drs://alias-1",
		},
		{
			name: "access denied",
			db: &coreTestDB{
				MockDatabase: &testutils.MockDatabase{
					Objects: map[string]*drs.DrsObject{
						"obj-3": {Id: "obj-3"},
					},
					ObjectAuthz: map[string]map[string][]string{
						"obj-3": {"a": {}},
					},
				},
			},
			ident:   "obj-3",
			method:  "delete",
			ctx:     buildGen3Context(map[string]map[string]bool{"/programs/b": {"delete": true}}),
			wantErr: common.ErrUnauthorized,
		},
		{
			name: "not found",
			db: &coreTestDB{
				MockDatabase: &testutils.MockDatabase{
					Objects: map[string]*drs.DrsObject{},
				},
			},
			ident:   "missing",
			method:  "read",
			ctx:     buildGen3Context(map[string]map[string]bool{"/data_file": {"read": true}}),
			wantErr: common.ErrNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			om := NewObjectManager(tc.db, &capturingURLManager{})
			obj, err := om.GetObject(tc.ctx, tc.ident, tc.method)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected error %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if obj.Id != tc.wantID {
				t.Fatalf("expected id %q, got %q", tc.wantID, obj.Id)
			}
			if obj.SelfUri != tc.wantURI {
				t.Fatalf("expected self uri %q, got %q", tc.wantURI, obj.SelfUri)
			}
		})
	}
}

func TestObjectManagerDeleteResolveAndSignDelegation(t *testing.T) {
	t.Run("delete by scope filters unauthorized objects", func(t *testing.T) {
		db := &coreTestDB{
			MockDatabase: &testutils.MockDatabase{
				Objects: map[string]*drs.DrsObject{
					"obj-a": {Id: "obj-a"},
					"obj-b": {Id: "obj-b"},
				},
				ObjectAuthz: map[string]map[string][]string{
					"obj-a": {"a": {"one"}},
					"obj-b": {"a": {"two"}},
				},
			},
		}
		om := NewObjectManager(db, &capturingURLManager{})
		ctx := buildGen3Context(map[string]map[string]bool{
			"/programs/a/projects/one": {"delete": true},
		})

		count, err := om.DeleteBulkByScope(ctx, "a", "")
		if err != nil {
			t.Fatalf("DeleteBulkByScope failed: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 deletion, got %d", count)
		}
		if _, ok := db.Objects["obj-a"]; ok {
			t.Fatalf("expected obj-a to be deleted")
		}
		if _, ok := db.Objects["obj-b"]; !ok {
			t.Fatalf("expected obj-b to remain")
		}
	})

	t.Run("resolve bucket uses configured credentials", func(t *testing.T) {
		db := &coreTestDB{
			MockDatabase: &testutils.MockDatabase{},
			creds: []models.S3Credential{
				{Bucket: "default-bucket", Provider: "s3"},
				{Bucket: "secondary", Provider: "s3"},
			},
		}
		om := NewObjectManager(db, &capturingURLManager{})

		got, err := om.ResolveBucket(context.Background(), "")
		if err != nil {
			t.Fatalf("ResolveBucket failed: %v", err)
		}
		if got != "default-bucket" {
			t.Fatalf("expected default bucket, got %q", got)
		}

		got, err = om.ResolveBucket(context.Background(), "secondary")
		if err != nil {
			t.Fatalf("ResolveBucket explicit bucket failed: %v", err)
		}
		if got != "secondary" {
			t.Fatalf("expected secondary bucket, got %q", got)
		}

		if _, err := om.ResolveBucket(context.Background(), "missing"); err == nil || !strings.Contains(err.Error(), `bucket "missing" not configured`) {
			t.Fatalf("expected error for unknown bucket")
		}
	})

	t.Run("signing delegates resolved bucket", func(t *testing.T) {
		db := &coreTestDB{
			MockDatabase: &testutils.MockDatabase{},
		}
		um := &capturingURLManager{}
		om := NewObjectManager(db, um)

		signed, err := om.SignURL(context.Background(), "s3://bucket-a/path/to/object", urlmanager.SignOptions{})
		if err != nil {
			t.Fatalf("SignURL failed: %v", err)
		}
		if signed != "signed:s3://bucket-a/path/to/object" {
			t.Fatalf("unexpected signed url: %s", signed)
		}
		if um.signURLBucket != "bucket-a" {
			t.Fatalf("expected bucket-a, got %q", um.signURLBucket)
		}

		partURL, err := om.SignDownloadPart(context.Background(), "bucket-b", "s3://bucket-b/path/to/object", 10, 20, urlmanager.SignOptions{})
		if err != nil {
			t.Fatalf("SignDownloadPart failed: %v", err)
		}
		if partURL != "download:s3://bucket-b/path/to/object" {
			t.Fatalf("unexpected download part url: %s", partURL)
		}
		if um.signDownloadBucket != "bucket-b" {
			t.Fatalf("expected download bucket bucket-b, got %q", um.signDownloadBucket)
		}

		uploadID, err := om.InitMultipartUpload(context.Background(), "bucket-c", "path/to/object")
		if err != nil {
			t.Fatalf("InitMultipartUpload failed: %v", err)
		}
		if uploadID != "upload-id" {
			t.Fatalf("unexpected upload id: %s", uploadID)
		}
		part, err := om.SignMultipartPart(context.Background(), "bucket-c", "path/to/object", uploadID, 3)
		if err != nil {
			t.Fatalf("SignMultipartPart failed: %v", err)
		}
		if part != "part:path/to/object" {
			t.Fatalf("unexpected part url: %s", part)
		}
		if err := om.CompleteMultipartUpload(context.Background(), "bucket-c", "path/to/object", uploadID, []urlmanager.MultipartPart{{PartNumber: 3, ETag: "etag"}}); err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}
		if um.partBucket != "bucket-c" || um.completeBucket != "bucket-c" {
			t.Fatalf("expected multipart delegation to bucket-c, got part=%q complete=%q", um.partBucket, um.completeBucket)
		}
	})
}
