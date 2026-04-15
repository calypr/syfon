package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type openAPIDoc struct {
	Paths map[string]map[string]interface{} `yaml:"paths"`
}

// TestOpenAPISpecRoutesRegistered ensures every documented OpenAPI method/path
// is actually mounted on the runtime router.
func TestOpenAPISpecRoutesRegistered(t *testing.T) {
	router := buildMockServerRouter()

	req := httptest.NewRequest(http.MethodGet, "/index/openapi.yaml", nil)
	resp, err := router.Test(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("failed to load merged openapi.yaml: status=%d body=%s", resp.StatusCode, string(body))
	}

	var spec openAPIDoc
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if err := yaml.Unmarshal(body, &spec); err != nil {
		t.Fatalf("failed to parse openapi yaml: %v", err)
	}
	if len(spec.Paths) == 0 {
		t.Fatal("merged openapi spec has no paths")
	}

	endpoints := collectEndpoints(t, router)
	routeSet := make(map[string]struct{}, len(endpoints))
	for _, ep := range endpoints {
		routeSet[strings.ToUpper(ep.Method)+" "+normalizeRoutePattern(ep.Template)] = struct{}{}
	}

	supportedOps := map[string]struct{}{
		"get":     {},
		"post":    {},
		"put":     {},
		"delete":  {},
		"patch":   {},
		"options": {},
		"head":    {},
	}

	missing := make([]string, 0)
	for path, ops := range spec.Paths {
		for op := range ops {
			if _, ok := supportedOps[strings.ToLower(op)]; !ok {
				continue
			}
			key := strings.ToUpper(op) + " " + path
			prefixedKey := strings.ToUpper(op) + " " + "/ga4gh/drs/v1" + path
			if _, ok := routeSet[key]; !ok {
				if _, okp := routeSet[prefixedKey]; okp {
					continue
				}
				missing = append(missing, key)
			}
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("openapi routes missing from runtime router:\n%s", strings.Join(missing, "\n"))
	}
}

func normalizeRoutePattern(path string) string {
	return pathVarPattern.ReplaceAllString(path, `{$1}`)
}
