package docs

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/gorilla/mux"
	"gopkg.in/yaml.v3"
)

const swaggerUIHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DRS Server API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        url: "/index/openapi.yaml",
        dom_id: "#swagger-ui"
      });
    };
  </script>
</body>
</html>
`

// RegisterSwaggerRoutes adds Swagger/OpenAPI docs endpoints.
func RegisterSwaggerRoutes(router *mux.Router) {
	router.HandleFunc("/index/swagger", handleSwaggerUI).Methods(http.MethodGet)
	router.HandleFunc("/index/swagger/", handleSwaggerUI).Methods(http.MethodGet)
	// OpenAPI is intentionally exposed only under /index for proxy compatibility.
	router.HandleFunc("/index/openapi.yaml", handleOpenAPISpec).Methods(http.MethodGet)
	router.HandleFunc("/index/openapi-lfs.yaml", handleLFSOpenAPISpec).Methods(http.MethodGet)
	router.HandleFunc("/index/openapi-bucket.yaml", handleBucketOpenAPISpec).Methods(http.MethodGet)
	router.HandleFunc("/index/openapi-internal.yaml", handleInternalOpenAPISpec).Methods(http.MethodGet)
}

func handleSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUIHTML))
}

func handleOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	merged, err := buildMergedOpenAPISpec()
	if err != nil {
		http.Error(w, "OpenAPI spec file not found: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(merged)
}

func handleLFSOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	specPath, ok := findLFSOpenAPISpecPath()
	if !ok {
		http.Error(w, "LFS OpenAPI spec file not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	http.ServeFile(w, r, specPath)
}

func handleBucketOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	specPath, ok := findBucketOpenAPISpecPath()
	if !ok {
		http.Error(w, "Bucket OpenAPI spec file not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	http.ServeFile(w, r, specPath)
}

func handleInternalOpenAPISpec(w http.ResponseWriter, r *http.Request) {
	specPath, ok := findNamedOpenAPISpecPath("internal.openapi.yaml")
	if !ok {
		http.Error(w, "Internal OpenAPI spec file not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	http.ServeFile(w, r, specPath)
}

func findOpenAPISpecPath() (string, bool) {
	candidates := []string{
		"apigen/api/openapi.yaml",
		filepath.Join(filepath.Dir(os.Args[0]), "apigen", "api", "openapi.yaml"),
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "apigen", "api", "openapi.yaml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func findLFSOpenAPISpecPath() (string, bool) {
	candidates := []string{
		"apigen/api/lfs.openapi.yaml",
		filepath.Join(filepath.Dir(os.Args[0]), "apigen", "api", "lfs.openapi.yaml"),
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "apigen", "api", "lfs.openapi.yaml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func findCompatOpenAPISpecPath() (string, bool) {
	candidates := []string{
		"apigen/api/compat.openapi.yaml",
		filepath.Join(filepath.Dir(os.Args[0]), "apigen", "api", "compat.openapi.yaml"),
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "apigen", "api", "compat.openapi.yaml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func findBucketOpenAPISpecPath() (string, bool) {
	candidates := []string{
		"apigen/api/bucket.openapi.yaml",
		filepath.Join(filepath.Dir(os.Args[0]), "apigen", "api", "bucket.openapi.yaml"),
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "apigen", "api", "bucket.openapi.yaml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func findNamedOpenAPISpecPath(fileName string) (string, bool) {
	candidates := []string{
		filepath.Join("apigen", "api", fileName),
		filepath.Join(filepath.Dir(os.Args[0]), "apigen", "api", fileName),
	}

	if _, thisFile, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "apigen", "api", fileName))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func buildMergedOpenAPISpec() ([]byte, error) {
	drsPath, ok := findOpenAPISpecPath()
	if !ok {
		return nil, fmt.Errorf("DRS spec missing")
	}
	lfsPath, ok := findLFSOpenAPISpecPath()
	if !ok {
		return nil, fmt.Errorf("LFS spec missing")
	}

	drsSpec, err := loadSpecYAML(drsPath)
	if err != nil {
		return nil, err
	}
	lfsSpec, err := loadSpecYAML(lfsPath)
	if err != nil {
		return nil, err
	}
	merged := drsSpec
	mergeSpecSection(merged, lfsSpec, "paths")
	mergeSpecSection(merged, lfsSpec, "components")
	if bucketPath, ok := findBucketOpenAPISpecPath(); ok {
		if bucketSpec, err := loadSpecYAML(bucketPath); err == nil {
			mergeSpecSection(merged, bucketSpec, "paths")
			mergeSpecSection(merged, bucketSpec, "components")
		}
	}
	for _, extra := range []string{
		"metrics.openapi.yaml",
		"internal.openapi.yaml",
	} {
		if p, ok := findNamedOpenAPISpecPath(extra); ok {
			if s, err := loadSpecYAML(p); err == nil {
				mergeSpecSection(merged, s, "paths")
				mergeSpecSection(merged, s, "components")
			}
		}
	}
	// Compatibility spec is optional; merge it if present.
	if compatPath, ok := findCompatOpenAPISpecPath(); ok {
		if compatSpec, err := loadSpecYAML(compatPath); err == nil {
			mergeSpecSection(merged, compatSpec, "paths")
			mergeSpecSection(merged, compatSpec, "components")
		}
	}

	out, err := yaml.Marshal(merged)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func loadSpecYAML(path string) (map[string]interface{}, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc map[string]interface{}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func mergeSpecSection(dst map[string]interface{}, src map[string]interface{}, section string) {
	srcVal, ok := src[section]
	if !ok {
		return
	}
	srcMap, ok := srcVal.(map[string]interface{})
	if !ok {
		return
	}

	dstVal, ok := dst[section]
	if !ok {
		dst[section] = srcMap
		return
	}
	dstMap, ok := dstVal.(map[string]interface{})
	if !ok {
		dst[section] = srcMap
		return
	}
	deepMerge(dstMap, srcMap)
}

func deepMerge(dst map[string]interface{}, src map[string]interface{}) {
	for k, v := range src {
		if existing, ok := dst[k]; ok {
			existingMap, existingOK := existing.(map[string]interface{})
			vMap, vOK := v.(map[string]interface{})
			if existingOK && vOK {
				deepMerge(existingMap, vMap)
				dst[k] = existingMap
				continue
			}
		}
		dst[k] = v
	}
}
