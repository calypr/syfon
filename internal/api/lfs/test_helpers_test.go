package lfs

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/calypr/syfon/internal/core"

	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

type fiberTestRouter struct {
	app *fiber.App
}

func ptr[T any](v T) *T { return &v }

func (r *fiberTestRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	resp, err := r.app.Test(req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer resp.Body.Close()
	for k, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func newLFSRouterWithOptions(opts Options) (*fiberTestRouter, *testutils.MockDatabase) {
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{},
	}
	storageFake := &lfsStorageFake{}
	app := fiber.New()
	deps := newLFSDependencies(db)
	deps.Storage = core.StoragePorts{Access: storageFake}
	om := core.NewObjectManager(deps)
	RegisterLFSRoutes(app, om, opts)
	return &fiberTestRouter{app: app}, db
}

func newLFSRouter() (*fiberTestRouter, *testutils.MockDatabase) {
	return newLFSRouterWithOptions(DefaultOptions())
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func resolveObjectForOID(ctx context.Context, database *testutils.MockDatabase, oid string) (*objects.Record, error) {
	om := core.NewObjectManager(newLFSDependencies(database))
	return om.GetObject(ctx, oid, "")
}

type lfsStorageFake struct {
	uploadURL      string
	accessRequests []storage.AccessRequest
	initCalled     int
	initBucket     string
	initKey        string
	partBucket     string
	partKey        string
	partUploadID   string
	partNumber     int32
	completeCalled int
	completeBucket string
	completeKey    string
	completeID     string
	completeParts  []storage.CompletedPart
}

func (m *lfsStorageFake) Access(_ context.Context, request storage.AccessRequest) (storage.Access, error) {
	m.accessRequests = append(m.accessRequests, request)
	suffix := "?signed=true"
	if request.Options.Method == http.MethodPut || request.Options.Method == http.MethodPost {
		suffix += "&upload=true"
	}
	return storage.Access{Location: request.Target.Location + suffix}, nil
}

func (m *lfsStorageFake) BeginMultipart(_ context.Context, target storage.ObjectTarget) (storage.UploadID, error) {
	m.initCalled++
	m.initBucket = target.Bucket
	m.initKey = target.Key
	return storage.UploadID("mock-upload-id"), nil
}

func (m *lfsStorageFake) AccessMultipartPart(_ context.Context, request storage.MultipartPartRequest) (storage.Access, error) {
	m.partBucket = request.Target.Bucket
	m.partKey = request.Target.Key
	m.partUploadID = string(request.UploadID)
	m.partNumber = request.PartNumber
	if m.uploadURL != "" {
		return storage.Access{Location: m.uploadURL}, nil
	}
	return storage.Access{Location: fmt.Sprintf("s3://%s/%s?uploadId=%s&partNumber=%d", request.Target.Bucket, request.Target.Key, request.UploadID, request.PartNumber)}, nil
}

func (m *lfsStorageFake) CompleteMultipart(_ context.Context, request storage.CompleteMultipartRequest) error {
	m.completeCalled++
	m.completeBucket = request.Target.Bucket
	m.completeKey = request.Target.Key
	m.completeID = string(request.UploadID)
	m.completeParts = append([]storage.CompletedPart(nil), request.Parts...)
	return nil
}

var _ core.StorageAccess = (*lfsStorageFake)(nil)
var _ core.StorageMultipart = (*lfsStorageFake)(nil)
