package buckets

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/storage"
	"github.com/gofiber/fiber/v3"
)

func ptr[T any](v T) *T { return &v }

type internalDRSStorageFake struct {
	mu sync.Mutex

	bucket         string
	key            string
	signURL        string
	signID         string
	signOpts       storage.AccessOptions
	uploadURL      string
	probeFn        func(context.Context, []storage.ProbeTarget) []storage.ProbeResult
	inventoryFn    func(context.Context, storage.InventoryRequest) (storage.InventoryResult, error)
	deleteFn       func(context.Context, []storage.DeleteTarget) error
	completeErr    error
	inventoryCalls []storage.InventoryRequest
	deleteCalls    [][]storage.DeleteTarget
	completeParts  []storage.CompletedPart
}

type internalDRSProbeFake struct {
	probeFn func(context.Context, []storage.ProbeTarget) []storage.ProbeResult
}

type internalDRSInventoryFake struct {
	mu             sync.Mutex
	inventoryFn    func(context.Context, storage.InventoryRequest) (storage.InventoryResult, error)
	inventoryCalls []storage.InventoryRequest
}

type internalDRSDeleteFake struct {
	mu          sync.Mutex
	deleteFn    func(context.Context, []storage.DeleteTarget) error
	deleteCalls [][]storage.DeleteTarget
}

func withTestAuthzContext(req *http.Request, mode string, privileges map[string]map[string]bool) *http.Request {
	return req.WithContext(dataTestAuthContext(req.Context(), mode, mode == "gen3", privileges))
}

func dataTestAuthContext(base context.Context, mode string, authHeader bool, privileges map[string]map[string]bool) context.Context {
	sessionMode := mode
	if mode == "local-authz" {
		sessionMode = "local"
	}
	session := access.NewSession(sessionMode)
	session.AuthHeaderPresent = authHeader
	session.AuthzEnforced = sessionMode == "gen3" || mode == "local-authz"
	session.SetAuthorizations(nil, privileges, session.AuthzEnforced)
	return access.WithSession(base, session)
}

func policyTestContext(mode string, authHeader bool, privileges map[string]map[string]bool) context.Context {
	session := access.NewSession(mode)
	session.AuthHeaderPresent = authHeader
	session.AuthzEnforced = mode == "gen3" || mode == "local"
	session.SetAuthorizations(nil, privileges, session.AuthzEnforced)
	return access.WithSession(context.Background(), session)
}

func (m *internalDRSStorageFake) Access(_ context.Context, request storage.AccessRequest) (storage.Access, error) {
	m.mu.Lock()
	m.signID = request.Target.AccessID
	m.signURL = request.Target.Location
	m.signOpts = request.Options
	m.mu.Unlock()
	suffix := "?signed=true"
	if strings.EqualFold(strings.TrimSpace(request.Options.Method), http.MethodPut) || strings.EqualFold(strings.TrimSpace(request.Options.Method), http.MethodPost) {
		suffix += "&upload=true"
	}
	if request.Range != nil {
		suffix += fmt.Sprintf("&range=%d-%d", request.Range.Start, request.Range.End)
	}
	return storage.Access{Location: request.Target.Location + suffix}, nil
}

func (m *internalDRSStorageFake) BeginMultipart(_ context.Context, target storage.ObjectTarget) (storage.UploadID, error) {
	m.mu.Lock()
	m.bucket = target.Bucket
	m.key = target.Key
	m.mu.Unlock()
	return storage.UploadID("mock-upload-id"), nil
}

func (m *internalDRSStorageFake) AccessMultipartPart(_ context.Context, request storage.MultipartPartRequest) (storage.Access, error) {
	m.mu.Lock()
	m.bucket = request.Target.Bucket
	m.key = request.Target.Key
	m.mu.Unlock()
	return storage.Access{Location: fmt.Sprintf("s3://%s/%s?uploadId=%s&partNumber=%d", request.Target.Bucket, request.Target.Key, request.UploadID, request.PartNumber)}, nil
}

func (m *internalDRSStorageFake) CompleteMultipart(_ context.Context, request storage.CompleteMultipartRequest) error {
	m.mu.Lock()
	m.bucket = request.Target.Bucket
	m.key = request.Target.Key
	m.completeParts = append([]storage.CompletedPart(nil), request.Parts...)
	err := m.completeErr
	m.mu.Unlock()
	return err
}

func (m *internalDRSStorageFake) Probe(ctx context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
	if m.probeFn != nil {
		return m.probeFn(ctx, targets)
	}
	results := make([]storage.ProbeResult, len(targets))
	for i, target := range targets {
		results[i] = storage.ProbeResult{
			ID:     target.ID,
			Target: target.Target,
			Metadata: storage.ObjectMetadata{
				Provider: "s3",
				Bucket:   target.Target.Bucket,
				Key:      target.Target.Key,
				Path:     target.Target.Key,
			},
		}
	}
	return results
}

func (m *internalDRSStorageFake) Inventory(ctx context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
	m.mu.Lock()
	m.inventoryCalls = append(m.inventoryCalls, request)
	m.mu.Unlock()
	if m.inventoryFn != nil {
		return m.inventoryFn(ctx, request)
	}
	return storage.InventoryResult{Complete: true}, nil
}

func (m *internalDRSStorageFake) DeleteExact(ctx context.Context, targets []storage.DeleteTarget) error {
	m.mu.Lock()
	m.deleteCalls = append(m.deleteCalls, append([]storage.DeleteTarget(nil), targets...))
	m.mu.Unlock()
	if m.deleteFn != nil {
		return m.deleteFn(ctx, targets)
	}
	return nil
}

func (m *internalDRSStorageFake) InvalidateBucket(string) {}

func (f *internalDRSProbeFake) Probe(ctx context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
	if f.probeFn != nil {
		return f.probeFn(ctx, targets)
	}
	return nil
}

func (f *internalDRSInventoryFake) Inventory(ctx context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
	f.mu.Lock()
	f.inventoryCalls = append(f.inventoryCalls, request)
	f.mu.Unlock()
	if f.inventoryFn != nil {
		return f.inventoryFn(ctx, request)
	}
	return storage.InventoryResult{Complete: true}, nil
}

func (f *internalDRSDeleteFake) DeleteExact(ctx context.Context, targets []storage.DeleteTarget) error {
	f.mu.Lock()
	f.deleteCalls = append(f.deleteCalls, append([]storage.DeleteTarget(nil), targets...))
	f.mu.Unlock()
	if f.deleteFn != nil {
		return f.deleteFn(ctx, targets)
	}
	return nil
}

func doInternalDRSTestRequest(req *http.Request, fixture internalDRSTestFixture) *httptest.ResponseRecorder {
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(req.Context())
		return c.Next()
	})
	RegisterRoutes(app, fixture.bucketService)

	rr := httptest.NewRecorder()
	resp, err := app.Test(req)
	if err != nil {
		rr.WriteHeader(http.StatusInternalServerError)
		_, _ = rr.WriteString(err.Error())
		return rr
	}
	defer resp.Body.Close()
	for k, vals := range resp.Header {
		for _, v := range vals {
			rr.Header().Add(k, v)
		}
	}
	rr.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(rr, resp.Body)
	return rr
}

func doInternalDRSTestRequestWithAlias(req *http.Request, fixture internalDRSTestFixture, method string, pattern string, handler fiber.Handler) *httptest.ResponseRecorder {
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(req.Context())
		return c.Next()
	})
	RegisterRoutes(app, fixture.bucketService)
	app.Add([]string{method}, pattern, handler)

	rr := httptest.NewRecorder()
	resp, err := app.Test(req)
	if err != nil {
		rr.WriteHeader(http.StatusInternalServerError)
		_, _ = rr.WriteString(err.Error())
		return rr
	}
	defer resp.Body.Close()
	for k, vals := range resp.Header {
		for _, v := range vals {
			rr.Header().Add(k, v)
		}
	}
	rr.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(rr, resp.Body)
	return rr
}
