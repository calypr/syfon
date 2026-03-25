package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/internal/api/admin"
	coreapi "github.com/calypr/drs-server/internal/api/coreapi"
	"github.com/calypr/drs-server/internal/api/docs"
	"github.com/calypr/drs-server/internal/api/internaldrs"
	"github.com/calypr/drs-server/internal/api/lfs"
	"github.com/calypr/drs-server/internal/api/metrics"
	"github.com/calypr/drs-server/internal/api/middleware"
	"github.com/calypr/drs-server/service"
	"github.com/calypr/drs-server/testutils"
	"github.com/gorilla/mux"
)

type endpointCase struct {
	Method   string
	Template string
}

var pathVarPattern = regexp.MustCompile(`\{([^}]+)\}`)

func TestAllRegisteredEndpoints_WithMocks(t *testing.T) {
	router := buildMockServerRouter()

	endpoints := collectEndpoints(t, router)
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

			match := &mux.RouteMatch{}
			if ok := router.Match(req, match); !ok {
				t.Fatalf("router did not match request for %s %s", ep.Method, path)
			}

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			if rr.Code <= 0 {
				t.Fatalf("invalid status code for %s %s: %d", ep.Method, path, rr.Code)
			}
			if rr.Code == http.StatusMethodNotAllowed {
				t.Fatalf("unexpected 405 for matched route %s %s", ep.Method, path)
			}
		})
	}

	// Critical endpoints used by clients should always be present.
	required := []endpointCase{
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/register"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/access"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/access-methods"},
		{Method: http.MethodPost, Template: "/ga4gh/drs/v1/objects/delete"},
		{Method: http.MethodPost, Template: "/info/lfs/objects/batch"},
		{Method: http.MethodPost, Template: "/index/bulk/sha256/validity"},
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

func buildMockServerRouter() *mux.Router {
	database := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-1": {
				Id:          "sha-1",
				Name:        "mock-object",
				Size:        1,
				Version:     "1",
				Description: "mock",
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: "sha-1"},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type:      "s3",
						AccessId:  "s3",
						AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://test-bucket-1/sha-1"},
						Authorizations: drs.AccessMethodAuthorizations{
							BearerAuthIssuers: []string{"/data_file"},
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

	objectsController := drs.NewObjectsAPIController(svc)
	serviceInfoController := drs.NewServiceInfoAPIController(svc)
	uploadRequestController := drs.NewUploadRequestAPIController(svc)

	router := mux.NewRouter().StrictSlash(true)
	registerAPIRoutes(router, objectsController, serviceInfoController, uploadRequestController)

	// Local mode for deterministic no-network auth behavior in tests.
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	authzMiddleware := middleware.NewAuthzMiddleware(logger, "local", "", "")
	requestIDMiddleware := middleware.NewRequestIDMiddleware(logger)
	router.Use(requestIDMiddleware.Middleware)
	router.Use(authzMiddleware.Middleware)

	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	admin.RegisterAdminRoutes(router, database, uM)
	docs.RegisterSwaggerRoutes(router)
	coreapi.RegisterCoreRoutes(router, database)
	metrics.RegisterMetricsRoutes(router, database)
	internaldrs.RegisterInternalIndexRoutes(router, database)
	internaldrs.RegisterInternalDataRoutes(router, database, uM)
	lfs.RegisterLFSRoutes(router, database, uM)
	return router
}

func collectEndpoints(t *testing.T, router *mux.Router) []endpointCase {
	t.Helper()
	out := make([]endpointCase, 0, 64)
	if err := router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		methods, err := route.GetMethods()
		if err != nil {
			return nil
		}
		template, err := route.GetPathTemplate()
		if err != nil || template == "" {
			return nil
		}
		for _, m := range methods {
			if m == http.MethodHead {
				continue
			}
			out = append(out, endpointCase{Method: m, Template: template})
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to walk routes: %v", err)
	}
	return out
}

func materializePath(template string) string {
	return pathVarPattern.ReplaceAllStringFunc(template, func(v string) string {
		name := strings.TrimSuffix(strings.TrimPrefix(v, "{"), "}")
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
	case "/ga4gh/drs/v1/objects/{object_id}/checksums":
		return []byte(`{"checksums":[{"type":"md5","checksum":"md5sum"}]}`), "application/json"
	case "/ga4gh/drs/v1/objects/delete":
		return []byte(`{"bulk_object_ids":["sha-1"],"delete_storage_data":false}`), "application/json"
	case "/ga4gh/drs/v1/objects/{object_id}/delete":
		return []byte(`{"delete_storage_data":false}`), "application/json"
	case "/ga4gh/drs/v1/upload-request":
		return []byte(`{"requests":[{"size":1,"checksums":[{"type":"sha256","checksum":"sha-1"}],"name":"obj.bin"}]}`), "application/json"
	case "/admin/credentials":
		if method == http.MethodPut {
			return []byte(`{"bucket":"test-bucket-2","region":"us-east-1","access_key":"k","secret_key":"s"}`), "application/json"
		}
	case "/admin/sign_url":
		return []byte(`{"url":"s3://test-bucket-1/sha-1","method":"GET"}`), "application/json"
	case "/index/v1/sha256/validity", "/index/bulk/sha256/validity":
		return []byte(`{"sha256":["sha-1"]}`), "application/json"
	case "/index/bulk/hashes":
		return []byte(`{"hashes":["sha-1"]}`), "application/json"
	case "/index/bulk":
		return []byte(`{"records":[{"did":"sha-1","hashes":{"sha256":"sha-1"},"size":1,"urls":["s3://test-bucket-1/sha-1"],"authz":["/data_file"]}]}`), "application/json"
	case "/index/bulk/documents":
		return []byte(`["sha-1"]`), "application/json"
	case "/data/upload", "/data/upload/{file_id}":
		if template == "/data/upload" {
			return []byte(`{"guid":"sha-1","authz":["/data_file"]}`), "application/json"
		}
		return []byte(`{}`), "application/json"
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
	case "/info/lfs/objects/batch":
		return []byte(`{"operation":"download","objects":[{"oid":"sha-1","size":1}]}`), "application/json"
	case "/info/lfs/objects/metadata":
		return []byte(`{"candidates":[{"name":"obj","size":1,"checksums":[{"type":"sha256","checksum":"sha-1"}],"access_methods":[{"type":"s3","access_url":{"url":"s3://test-bucket-1/sha-1"},"authorizations":{"bearer_auth_issuers":["/data_file"]}}]}]}`), "application/json"
	case "/info/lfs/verify":
		return []byte(`{"oid":"sha-1","size":1}`), "application/json"
	}
	return []byte(`{}`), "application/json"
}
