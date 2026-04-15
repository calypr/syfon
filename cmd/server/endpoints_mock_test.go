package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/db/core"
	coreapi "github.com/calypr/syfon/internal/api/coreapi"
	"github.com/calypr/syfon/internal/api/docs"
	"github.com/calypr/syfon/internal/api/internaldrs"
	"github.com/calypr/syfon/internal/api/lfs"
	"github.com/calypr/syfon/internal/api/metrics"
	"github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

type endpointCase struct {
	Method   string
	Template string
}

var pathVarPattern = regexp.MustCompile(`:([A-Za-z0-9_]+)`)

func TestAllRegisteredEndpoints_WithMocks(t *testing.T) {
	app := buildMockServerRouter()

	endpoints := collectEndpoints(t, app)
	if len(endpoints) == 0 {
		t.Fatal("no endpoints discovered from router")
	}

	seen := make(map[string]struct{}, len(endpoints))
	for _, ep := range endpoints {
		key := ep.Method + " " + ep.Template
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		t.Run(key, func(t *testing.T) {
			path := materializePath(ep.Template)
			body, contentType := requestBodyFor(ep.Method, ep.Template)
			req := httptest.NewRequest(ep.Method, path, bytes.NewReader(body))
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}
			if strings.HasPrefix(path, "/info/lfs/") {
				req.Header.Set("Accept", "application/vnd.git-lfs+json")
			}

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("test request failed: %v", err)
			}
			if resp.StatusCode <= 0 {
				t.Fatalf("invalid status code for %s %s: %d", ep.Method, path, resp.StatusCode)
			}
			if resp.StatusCode == http.StatusMethodNotAllowed {
				t.Fatalf("unexpected 405 for matched route %s %s", ep.Method, path)
			}
		})
	}

	required := []endpointCase{
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/register"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/access"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/access-methods"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/delete"},
		{Method: http.MethodPost, Template: "/info/lfs/objects/batch"},
		{Method: http.MethodPost, Template: "/index/bulk/sha256/validity"},
		{Method: http.MethodGet, Template: "/index/v1/metrics/summary"},
		{Method: http.MethodGet, Template: "/index/v1/metrics/files"},
	}
	for _, req := range required {
		if _, ok := seen[req.Method+" "+req.Template]; !ok {
			t.Fatalf("required endpoint missing from router: %s %s", req.Method, req.Template)
		}
	}
}

func TestAdminRoutesNotRegistered(t *testing.T) {
	app := buildMockServerRouter()

	reqSign := httptest.NewRequest(http.MethodPost, "/admin/sign_url", bytes.NewBufferString(`{"url":"s3://b/k","method":"GET"}`))
	reqSign.Header.Set("Content-Type", "application/json")
	respSign, err := app.Test(reqSign)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if respSign.StatusCode != http.StatusNotFound {
		t.Fatalf("expected /admin/sign_url to be absent (404), got %d", respSign.StatusCode)
	}

	reqCreds := httptest.NewRequest(http.MethodGet, "/admin/credentials", nil)
	respCreds, err := app.Test(reqCreds)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if respCreds.StatusCode != http.StatusNotFound {
		t.Fatalf("expected /admin/credentials to be absent (404), got %d", respCreds.StatusCode)
	}
}

func buildMockServerRouter() *fiber.App {
	database := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-1": {
				Id:          "sha-1",
				Name:        core.Ptr("mock-object"),
				Size:        1,
				Version:     core.Ptr("1"),
				Description: core.Ptr("mock"),
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "sha-1"}},
				AccessMethods: &[]drs.AccessMethod{
					{
						Type:      drs.AccessMethodTypeS3,
						AccessId:  core.Ptr("s3"),
						AccessUrl: &struct{ Headers *[]string `json:"headers,omitempty"`; Url string `json:"url"` }{Url: "s3://test-bucket-1/sha-1"},
						Authorizations: &struct {
							BearerAuthIssuers   *[]string `json:"bearer_auth_issuers,omitempty"`
							DrsObjectId         *string   `json:"drs_object_id,omitempty"`
							PassportAuthIssuers *[]string `json:"passport_auth_issuers,omitempty"`
							SupportedTypes      *[]drs.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
						}{
							BearerAuthIssuers: &[]string{"/data_file"},
						},
					},
				},
			},
		},
		ObjectAuthz: map[string][]string{
			"sha-1": {"/data_file"},
		},
		Credentials: map[string]core.S3Credential{
			"test-bucket-1": {
				Bucket:    "test-bucket-1",
				Region:    "us-east-1",
				AccessKey: "mock-key",
				SecretKey: "mock-secret",
			},
		},
	}
	uM := &testutils.MockUrlManager{}
	svc := service.NewObjectsAPIService(database, uM)
	app := fiber.New()

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	authzMiddleware := middleware.NewAuthzMiddleware(logger, "local", "", "")
	requestIDMiddleware := middleware.NewRequestIDMiddleware(logger)
	app.Use(requestIDMiddleware.FiberMiddleware())
	app.Use(authzMiddleware.FiberMiddleware())

	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.SendString("OK")
	})

	api := app.Group("/")
	strict := service.NewStrictServer(svc)
	drs.RegisterHandlersWithOptions(api, drs.NewStrictHandler(strict, nil), drs.FiberServerOptions{
		BaseURL: "/ga4gh/drs/v1",
	})
	docs.RegisterSwaggerRoutes(app)
	coreapi.RegisterCoreRoutes(api, database)
	metrics.RegisterMetricsRoutes(api, database)
	internaldrs.RegisterInternalIndexRoutes(api, database, uM)
	internaldrs.RegisterInternalDataRoutes(api, database, uM)
	lfs.RegisterLFSRoutes(api, database, uM)
	return app
}

func collectEndpoints(t *testing.T, app *fiber.App) []endpointCase {
	t.Helper()
	routes := app.GetRoutes(false)
	out := make([]endpointCase, 0, len(routes))
	for _, route := range routes {
		if route.Method == http.MethodHead {
			continue
		}
		out = append(out, endpointCase{Method: route.Method, Template: route.Path})
	}
	return out
}

func materializePath(template string) string {
	return pathVarPattern.ReplaceAllStringFunc(template, func(v string) string {
		name := strings.TrimPrefix(v, ":")
		switch name {
		case "object_id", "id", "did", "guid", "file_id", "oid":
			return "sha-1"
		case "access_id":
			return "s3"
		case "bucket":
			return "test-bucket-1"
		default:
			return "value"
		}
	})
}

func requestBodyFor(method, template string) ([]byte, string) {
	if method != http.MethodPost && method != http.MethodPut && method != http.MethodDelete {
		return nil, ""
	}

	switch template {
	case "/ga4gh/drs/v1/objects/register":
		return []byte(`{"candidates":[{"name":"obj","size":1,"checksums":[{"type":"sha256","checksum":"sha-1"}],"access_methods":[{"type":"s3","access_url":{"url":"s3://test-bucket-1/sha-1"},"authorizations":{"bearer_auth_issuers":["/data_file"]}}]}]}`), "application/json"
	case "/ga4gh/drs/v1/objects":
		return []byte(`{"bulk_object_ids":["sha-1"]}`), "application/json"
	case "/ga4gh/drs/v1/objects/access":
		return []byte(`{"bulk_object_access_ids":[{"bulk_object_id":"sha-1","bulk_access_ids":["s3"]}]}`), "application/json"
	case "/ga4gh/drs/v1/objects/access-methods":
		return []byte(`{"updates":[{"object_id":"sha-1","access_methods":[{"type":"s3","access_id":"s3","access_url":{"url":"s3://test-bucket-1/sha-1"}}]}]}`), "application/json"
	case "/ga4gh/drs/v1/objects/:object_id/checksums":
		return []byte(`{"checksums":[{"type":"md5","checksum":"md5sum"}]}`), "application/json"
	case "/ga4gh/drs/v1/objects/delete":
		return []byte(`{"bulk_object_ids":["sha-1"],"delete_storage_data":false}`), "application/json"
	case "/ga4gh/drs/v1/objects/:object_id/delete":
		return []byte(`{"delete_storage_data":false}`), "application/json"
	case "/ga4gh/drs/v1/upload-request":
		return []byte(`{"requests":[{"size":1,"checksums":[{"type":"sha256","checksum":"sha-1"}],"name":"obj.bin"}]}`), "application/json"
	case "/index/v1/sha256/validity", "/index/bulk/sha256/validity":
		return []byte(`{"sha256":["sha-1"]}`), "application/json"
	case "/index/bulk/hashes":
		return []byte(`{"hashes":["sha-1"]}`), "application/json"
	case "/index/bulk":
		return []byte(`{"records":[{"did":"sha-1","hashes":{"sha256":"sha-1"},"size":1,"urls":["s3://test-bucket-1/sha-1"],"authz":["/data_file"]}]}`), "application/json"
	case "/index/bulk/documents":
		return []byte(`["sha-1"]`), "application/json"
	case "/data/upload", "/data/upload/:file_id":
		if template == "/data/upload" {
			return []byte(`{"guid":"sha-1","authz":["/data_file"]}`), "application/json"
		}
		return []byte(`{}`), "application/json"
	case "/data/upload/bulk":
		return []byte(`{"requests":[{"file_id":"sha-1","bucket":"test-bucket-1","file_name":"sha-1"}]}`), "application/json"
	case "/data/multipart/init":
		return []byte(`{"guid":"sha-1","file_name":"sha-1","bucket":"test-bucket-1"}`), "application/json"
	case "/data/multipart/upload":
		return []byte(`{"key":"sha-1","bucket":"test-bucket-1","uploadId":"mock-upload-id","partNumber":1}`), "application/json"
	case "/data/multipart/complete":
		return []byte(`{"key":"sha-1","bucket":"test-bucket-1","uploadId":"mock-upload-id","parts":[{"PartNumber":1,"ETag":"etag-1"}]}`), "application/json"
	case "/data/buckets":
		if method == http.MethodPut {
			return []byte(`{"bucket":"test-bucket-3","region":"us-east-1","access_key":"k","secret_key":"s","endpoint":""}`), "application/json"
		}
		return []byte(`{}`), "application/json"
	case "/data/buckets/:bucket/scopes":
		return []byte(`{"organization":"cbds","project_id":"proj2"}`), "application/json"
	case "/info/lfs/objects/batch":
		return []byte(`{"operation":"download","objects":[{"oid":"sha-1","size":1}]}`), "application/json"
	case "/info/lfs/objects/metadata":
		return []byte(`{"candidates":[{"name":"obj","size":1,"checksums":[{"type":"sha256","checksum":"sha-1"}],"access_methods":[{"type":"s3","access_url":{"url":"s3://test-bucket-1/sha-1"},"authorizations":{"bearer_auth_issuers":["/data_file"]}}]}]}`), "application/json"
	case "/info/lfs/verify":
		return []byte(`{"oid":"sha-1","size":1}`), "application/json"
	case "/info/lfs/objects/:oid":
		return []byte(`{}`), "application/octet-stream"
	default:
		return []byte(`{}`), "application/json"
	}
}
