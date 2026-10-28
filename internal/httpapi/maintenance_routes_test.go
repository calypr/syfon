package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/bucketapi"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
	clientaccess "github.com/calypr/syfon/client/access"
	"github.com/calypr/syfon/internal/access"
	domainbuckets "github.com/calypr/syfon/internal/buckets"
	projectstorage "github.com/calypr/syfon/internal/projects/storage"
	providerstorage "github.com/calypr/syfon/internal/storage"
	"github.com/gofiber/fiber/v3"
)

const maintenanceRouteSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type maintenanceRouteProvider struct{}

func (maintenanceRouteProvider) Probe(_ context.Context, targets []providerstorage.ProbeTarget) []providerstorage.ProbeResult {
	results := make([]providerstorage.ProbeResult, 0, len(targets))
	for _, target := range targets {
		results = append(results, providerstorage.ProbeResult{
			ID:     target.ID,
			Target: target.Target,
			Metadata: providerstorage.ObjectMetadata{
				Provider:   "s3",
				Bucket:     target.Target.PhysicalBucket,
				Key:        target.Target.Key,
				SizeBytes:  0,
				MetaSHA256: maintenanceRouteSHA,
			},
		})
	}
	return results
}

func (maintenanceRouteProvider) Inventory(_ context.Context, request providerstorage.InventoryRequest) (providerstorage.InventoryResult, error) {
	return providerstorage.InventoryResult{
		Items: []providerstorage.ObjectMetadata{{
			Provider:   "s3",
			Bucket:     request.Target.PhysicalBucket,
			Key:        request.Prefix,
			SizeBytes:  0,
			MetaSHA256: maintenanceRouteSHA,
		}},
		Complete: true,
	}, nil
}

func newMaintenanceRouteApp(t *testing.T) *fiber.App {
	t.Helper()
	credentialStore := &bucketTestStore{Credentials: map[string]domainbuckets.Credential{
		"credential": {CredentialID: "credential", Bucket: "bucket", Provider: "s3"},
	}}
	bucketService := newInternalDRSObjectManager(credentialStore).bucketService
	storageService := projectstorage.NewService(projectstorage.Dependencies{
		Credentials: bucketService,
		Visibility:  bucketService,
		Providers:   projectstorage.Providers{Probe: maintenanceRouteProvider{}, Inventory: maintenanceRouteProvider{}},
	})
	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	RegisterRoutes(app, Dependencies{ProjectStorage: storageService}, Options{Internal: true})
	return app
}

type nativeProbeResolver struct{}

func (nativeProbeResolver) ResolveStorageScope(context.Context, string, string) (domainbuckets.StorageScope, error) {
	return domainbuckets.StorageScope{Provider: "s3", Bucket: "bucket", Prefix: "project-prefix"}, nil
}

type nativeCancelableProbe struct {
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (p *nativeCancelableProbe) Probe(ctx context.Context, targets []providerstorage.ProbeTarget) []providerstorage.ProbeResult {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	close(p.canceled)
	return []providerstorage.ProbeResult{{ID: targets[0].ID, Target: targets[0].Target, Err: ctx.Err()}}
}

type nativeProbeEvaluator struct{}

func (nativeProbeEvaluator) Evaluate(request access.EvaluationRequest) access.EvaluationResult {
	session := access.NewSession(request.Mode)
	if request.AuthHeader != "Bearer probe" {
		return access.EvaluationResult{Session: session, Decision: access.DecisionUnauthorized}
	}
	session.AuthHeaderPresent = true
	resource, _ := clientaccess.ResourcePath("org", "project")
	session.SetAuthorizations([]string{resource}, map[string]map[string]bool{resource: {"read": true}}, true)
	return access.EvaluationResult{Session: session, Decision: access.DecisionContinue}
}

func TestNativeHTTPBridgeCancelsActualInspectProviderOnTCPAbort(t *testing.T) {
	probe := &nativeCancelableProbe{started: make(chan struct{}), canceled: make(chan struct{})}
	service := projectstorage.NewService(projectstorage.Dependencies{
		ScopeResolver: nativeProbeResolver{},
		Providers:     projectstorage.Providers{Probe: probe},
	})
	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	server := httptest.NewServer(NewNativeHTTPHandler(app, func() {
		RegisterRoutes(app, Dependencies{
			ProjectStorage: service,
			Authorization:  AuthorizationHandler(AuthzOptions{Mode: "gen3", Evaluator: nativeProbeEvaluator{}}),
			RequestIDs:     RequestIDHandler(nil),
		}, Options{Internal: true})
	}))
	t.Cleanup(server.Close)

	unauthorized, err := http.NewRequest(http.MethodPost, server.URL+"/data/inspect", strings.NewReader(`{"organization":"org","project":"project","key":"file.bin"}`))
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Header.Set("Content-Type", "application/json")
	unauthorized.Header.Set("X-Request-Id", "bridge-auth-check")
	unauthorizedResponse, err := server.Client().Do(unauthorized)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, unauthorizedResponse.Body)
	_ = unauthorizedResponse.Body.Close()
	if unauthorizedResponse.StatusCode != http.StatusUnauthorized || unauthorizedResponse.Header.Get("X-Request-Id") != "bridge-auth-check" {
		t.Fatalf("unauthorized response = (%d, %q), want 401 and propagated request ID", unauthorizedResponse.StatusCode, unauthorizedResponse.Header.Get("X-Request-Id"))
	}

	address := strings.TrimPrefix(server.URL, "http://")
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"organization":"org","project":"project","key":"file.bin"}`
	request := "POST /data/inspect HTTP/1.1\r\n" +
		"Host: " + address + "\r\n" +
		"Authorization: Bearer probe\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: " + strconv.Itoa(len(body)) + "\r\n" +
		"Connection: close\r\n\r\n" + body
	if _, err := io.WriteString(connection, request); err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	select {
	case <-probe.started:
	case <-time.After(5 * time.Second):
		_ = connection.Close()
		t.Fatal("project storage provider did not receive the inspect request")
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("TCP disconnect did not cancel the inspect provider context")
	}
}

func TestProjectDeleteRouteHasOneGeneratedOwner(t *testing.T) {
	app := fiber.New()
	RegisterRoutes(app, Dependencies{}, Options{Internal: true})

	const path = "/data/projects/:organization/:project_id"
	count := 0
	for _, methodRoutes := range app.Stack() {
		for _, route := range methodRoutes {
			if route.Method == http.MethodDelete && route.Path == path {
				count++
			}
		}
	}
	if count != 1 {
		t.Fatalf("DELETE %s route count = %d, want 1", path, count)
	}
}

type maintenanceCleanupObjects struct {
	deleted []string
}

func (f *maintenanceCleanupObjects) DeleteBulkByScope(_ context.Context, organization, project string) (int, error) {
	f.deleted = append(f.deleted, organization+"/"+project)
	return 2, nil
}

type maintenanceCleanupScopes struct {
	deleted []string
}

func (f *maintenanceCleanupScopes) ListBucketScopes(context.Context) ([]domainbuckets.Scope, error) {
	return []domainbuckets.Scope{{Organization: "org", ProjectID: "project", CredentialID: "credential"}}, nil
}

func (f *maintenanceCleanupScopes) DeleteBucketScope(_ context.Context, organization, project, credential, path string) error {
	f.deleted = append(f.deleted, strings.Join([]string{organization, project, credential, path}, "/"))
	return nil
}

func TestBucketProjectDeleteRouteRunsProjectCleanup(t *testing.T) {
	objects := &maintenanceCleanupObjects{}
	scopes := &maintenanceCleanupScopes{}
	app := fiber.New(fiber.Config{ErrorHandler: FiberErrorHandler})
	RegisterRoutes(app, Dependencies{ProjectStorage: projectstorage.NewService(projectstorage.Dependencies{
		ObjectCleanup: objects,
		ScopeCatalog:  scopes,
	})}, Options{Internal: true})

	request := httptest.NewRequest(http.MethodDelete, "/data/projects/org/project", nil)
	request = request.WithContext(dataTestAuthContext(request.Context(), "gen3", true, map[string]map[string]bool{
		"/programs/org/projects/project": {"delete": true},
	}))
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("DELETE project data: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, want 200: %s", response.StatusCode, payload)
	}
	var result bucketapi.DeleteProjectDataResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Organization != "org" || result.ProjectId != "project" || result.DeletedObjects != 2 || result.DeletedBucketScopes != 1 {
		t.Fatalf("response = %+v", result)
	}
	if len(objects.deleted) != 1 || objects.deleted[0] != "org/project" || len(scopes.deleted) != 1 || scopes.deleted[0] != "org/project/credential/" {
		t.Fatalf("cleanup calls = objects:%v scopes:%v", objects.deleted, scopes.deleted)
	}
}

func TestMaintenanceInspectBoundaryCharacterization(t *testing.T) {
	tests := []struct {
		name, path, body string
		status           int
		check            func(*testing.T, []byte)
	}{
		{
			name:   "single rejects validation without a validation response",
			path:   "/data/inspect",
			body:   `{"id":"single","object_url":" s3://bucket/prefix/file.txt ","organization":"ignored","project":"ignored","key":"ignored","scheme":"ignored","expected_name":"wrong.txt","expected_sha256":" sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA "}`,
			status: 400,
			check: func(t *testing.T, payload []byte) {
				if !strings.Contains(string(payload), "validation fields") {
					t.Fatalf("error response = %s", payload)
				}
			},
		},
		{
			name:   "bulk validates expected name and trimmed hash",
			path:   "/data/inspect/bulk",
			body:   `{"items":[{"id":"bulk","object_url":" s3://bucket/prefix/file.txt ","organization":"ignored","project":"ignored","key":"ignored","scheme":"ignored","expected_name":"wrong.txt","expected_sha256":" sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA "}]}`,
			status: 200,
			check: func(t *testing.T, payload []byte) {
				var response internalapi.InternalInspectObjectBulkResponse
				if err := json.Unmarshal(payload, &response); err != nil {
					t.Fatalf("decode response: %v", err)
				}
				if len(response.Items) != 1 {
					t.Fatalf("items = %+v", response.Items)
				}
				item := response.Items[0]
				if item.ValidationStatus != "mismatched" || item.Sha256Match == nil || !*item.Sha256Match || item.NameMatch == nil || *item.NameMatch || item.SizeMatch != nil {
					t.Fatalf("bulk item = %+v, want hash match and name mismatch", item)
				}
			},
		},
		{
			name:   "bulk explicit zero size differs from omitted size",
			path:   "/data/inspect/bulk",
			body:   `{"items":[{"id":"bulk-zero","object_url":"s3://bucket/prefix/file.txt","expected_name":"wrong.txt","expected_size_bytes":0}]}`,
			status: 200,
			check: func(t *testing.T, payload []byte) {
				var response internalapi.InternalInspectObjectBulkResponse
				if err := json.Unmarshal(payload, &response); err != nil {
					t.Fatalf("decode response: %v", err)
				}
				item := response.Items[0]
				if item.ValidationStatus != "mismatched" || item.SizeMatch == nil || !*item.SizeMatch || item.NameMatch == nil || *item.NameMatch {
					t.Fatalf("bulk item = %+v, want explicit zero-size match and name mismatch", item)
				}
			},
		},
		{
			name:   "bulk list validates expected SHA",
			path:   "/data/inspect/bulk-list",
			body:   `{"items":[{"id":"list","object_url":"s3://bucket/prefix/file.txt","organization":"invalid","project":"invalid","key":"invalid","scheme":"file","expected_name":"file.txt","expected_size_bytes":0,"expected_sha256":"sha256:not-the-remote-hash"}]}`,
			status: 200,
			check: func(t *testing.T, payload []byte) {
				var response internalapi.InternalInspectObjectBulkResponse
				if err := json.Unmarshal(payload, &response); err != nil {
					t.Fatalf("decode response: %v", err)
				}
				item := response.Items[0]
				if item.ValidationStatus != "mismatched" || item.SizeMatch == nil || !*item.SizeMatch || item.NameMatch == nil || !*item.NameMatch || item.Sha256Match == nil || *item.Sha256Match || item.MetaSha256 != maintenanceRouteSHA || !reflect.DeepEqual(item.ValidationMismatches, []string{"sha256_mismatch"}) {
					t.Fatalf("bulk-list item = %+v, want size/name matches and SHA mismatch", item)
				}
			},
		},
		{
			name:   "strict unknown field",
			path:   "/data/inspect/bulk",
			body:   `{"items":[{"object_url":"s3://bucket/prefix/file.txt","unexpected":true}]}`,
			status: 400,
			check: func(t *testing.T, payload []byte) {
				if !strings.Contains(string(payload), "unexpected") {
					t.Fatalf("error response = %s", payload)
				}
			},
		},
		{
			name:   "empty bulk items",
			path:   "/data/inspect/bulk",
			body:   `{"items":[]}`,
			status: 400,
		},
		{
			name:   "empty bulk-list items",
			path:   "/data/inspect/bulk-list",
			body:   `{"items":[]}`,
			status: 400,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newMaintenanceRouteApp(t)
			request := httptest.NewRequest("POST", test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response, err := app.Test(request)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			payload, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if response.StatusCode != test.status {
				t.Fatalf("status = %d, want %d: %s", response.StatusCode, test.status, payload)
			}
			if test.check != nil {
				test.check(t, payload)
			}
		})
	}
}
