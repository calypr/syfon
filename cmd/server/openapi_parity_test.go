package server

import (
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
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("failed to load merged openapi.yaml: status=%d body=%s", rr.Code, rr.Body.String())
	}

	var spec openAPIDoc
	if err := yaml.Unmarshal(rr.Body.Bytes(), &spec); err != nil {
		t.Fatalf("failed to parse openapi yaml: %v", err)
	}
	if len(spec.Paths) == 0 {
		t.Fatal("merged openapi spec has no paths")
	}

	endpoints := collectEndpoints(t, router)
	routeSet := make(map[string]struct{}, len(endpoints))
	for _, ep := range endpoints {
		routeSet[strings.ToUpper(ep.Method)+" "+ep.Template] = struct{}{}
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
